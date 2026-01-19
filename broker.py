import os
import uuid
import math
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import List, Optional, Dict, Any

import pandas as pd

from tinkoff.invest import (
    Client,
    CandleInterval,
    InstrumentIdType,
    OrderDirection,
    OrderType,
    Quotation,
    RequestError,
)
from tinkoff.invest.utils import now, quotation_to_decimal, decimal_to_quotation

from state import BotState
from journal import TradeJournal


@dataclass
class InstrumentInfo:
    ticker: str
    figi: str
    lot: int
    min_price_increment: float


class Broker:
    """
    T-Invest broker wrapper with:
      - sandbox/real routing for account-bound methods (positions/orders/operations)
      - robust ticker -> share resolution via share_by(TICKER, class_code)
      - candle polling (1m)
      - idempotent limit orders (client uid)
      - CSV trade journal (signals/orders/fills/cancels/rejects)
      - order execution polling via get_order_state (sandbox/real)

    IMPORTANT TYPE RULE:
      - internally we keep ALL prices as float
      - Decimal is used ONLY when constructing Quotation/MoneyValue for API calls
    """

    def __init__(self, client: Client, cfg: dict, notifier=None):
        self.client = client
        self.cfg = cfg
        self.state = BotState()
        self.notifier = notifier

        # internal throttles
        self._last_low_cash_warn: Dict[str, float] = {}

        os.makedirs("logs", exist_ok=True)

        self.logger = logging.getLogger("bot")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        log_path = cfg.get("log_file", "logs/bot.log")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        self.logger.addHandler(fh)
        self.logger.propagate = False

        self.currency = cfg.get("currency", "rub")
        self.use_sandbox = bool(cfg.get("use_sandbox", True))
        self.class_code = cfg.get("class_code", "TQBR")

        # retry/backoff
        self._retry_tries = int(cfg.get("retry_tries", 3))
        self._retry_sleep_min = float(cfg.get("retry_sleep_min", 1.0))
        self._retry_sleep_max = float(cfg.get("retry_sleep_max", 10.0))

        # cache figi -> InstrumentInfo
        self._figi_info: Dict[str, InstrumentInfo] = {}

        # last known cash from snapshot (to avoid extra API calls)
        self.last_cash_rub: float = 0.0

        # CSV journal
        self.journal = TradeJournal(cfg.get("trades_csv", "logs/trades.csv"))

    # ---------- logging ----------
    def log(self, msg: str):
        self.logger.info(msg)
        print(msg)

    def notify(self, text: str, throttle_sec: float = 0.0):
        """Best-effort Telegram notification (never raises)."""
        if not self.notifier:
            return
        try:
            self.notifier.send(text, throttle_sec=throttle_sec)
        except Exception:
            pass

    # ---------- account-wide snapshot (positions + orders) ----------
    def refresh_account_snapshot(self, account_id: str, figis: List[str]):
        """
        Pulls positions + orders once and updates state for the provided figis.
        This greatly reduces API calls vs calling sync_state() per figi.
        """
        self._ensure_day_rollover()
        figi_set = set(figis)

        # Positions (1 call)
        try:
            pos = self._call(self._positions_call(), account_id=account_id)

            # Cache available cash from the same response
            cash = 0.0
            for m in getattr(pos, "money", []) or []:
                try:
                    if getattr(m, "currency", None) == self.currency:
                        cash += float(self._to_float(m))
                except Exception:
                    pass
            self.last_cash_rub = float(cash)

            by_figi_lots: Dict[str, int] = {f: 0 for f in figi_set}
            for sec in getattr(pos, "securities", []):
                f = getattr(sec, "figi", "")
                if f in figi_set:
                    by_figi_lots[f] = int(self._to_float(getattr(sec, "balance", 0)))

            for f in figi_set:
                fs = self.state.get(f)
                prev_lots = int(fs.position_lots)
                fs.position_lots = int(by_figi_lots.get(f, 0))

                # Entry bookkeeping
                if prev_lots > 0 and int(fs.position_lots) == 0:
                    fs.entry_price = None
                    fs.entry_time = None
                if prev_lots == 0 and int(fs.position_lots) > 0:
                    if fs.entry_time is None:
                        fs.entry_time = now()
                    if fs.entry_price is None:
                        last = self.get_last_price(f)
                        if last is not None:
                            fs.entry_price = float(last)
        except Exception as e:
            self.log(f"[WARN] get_positions failed: {e}")

        # Orders (1 call)
        try:
            orders = self._call(self._orders_list_call(), account_id=account_id).orders
            active_by_figi: Dict[str, str] = {}
            for o in orders:
                f = getattr(o, "figi", "")
                if f in figi_set and f not in active_by_figi:
                    active_by_figi[f] = getattr(o, "order_id", "")

            for f in figi_set:
                fs = self.state.get(f)
                fs.active_order_id = active_by_figi.get(f) or None
        except Exception as e:
            self.log(f"[WARN] get_orders failed: {e}")

    # ---------- robust numeric converters ----------
    @staticmethod
    def _to_float(x: Any) -> float:
        """
        Safe converter for Quotation/MoneyValue-like objects and plain numbers.

        Fixes errors like: "'int' object has no attribute 'nano'"
        that happen when code expects Quotation/MoneyValue but gets 0 / int / float.
        """
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, Decimal):
            return float(x)

        # Quotation / MoneyValue from SDK: have .units and .nano
        units = getattr(x, "units", None)
        nano = getattr(x, "nano", None)
        if isinstance(units, int) and isinstance(nano, int):
            return float(units) + float(nano) / 1e9

        # fallback: try SDK helper, but guard against wrong types
        try:
            return float(quotation_to_decimal(x))
        except Exception:
            return 0.0

    # ---------- journal helpers ----------
    def _ticker_for_figi(self, figi: str) -> str:
        info = self._figi_info.get(figi)
        return info.ticker if info else ""

    # Public helpers for prettier logs
    def ticker_for_figi(self, figi: str) -> str:
        return self._ticker_for_figi(figi)

    def format_instrument(self, figi: str) -> str:
        """Human-friendly instrument label for logs/TG."""
        t = self._ticker_for_figi(figi)
        return f"{t} ({figi})" if t else figi

    def get_cached_cash_rub(self, account_id: str | None = None) -> float:
        """Returns last cash from snapshot; if unknown and account_id provided, falls back to API."""
        if self.last_cash_rub > 0:
            return float(self.last_cash_rub)
        if account_id:
            return float(self.get_cash_rub(account_id))
        return 0.0

    def journal_event(self, event: str, figi: str, **kwargs):
        self.journal.write(event=event, figi=figi, ticker=self._ticker_for_figi(figi), **kwargs)

    # ---------- day helpers ----------
    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo("UTC")).date().isoformat()

    def _ensure_day_rollover(self):
        today = self._today_key()
        if self.state.current_day != today:
            self.state.reset_day(today)

    # ---------- retry wrapper ----------
    def _call(self, fn, *args, **kwargs):
        sleep = self._retry_sleep_min
        for attempt in range(1, self._retry_tries + 1):
            try:
                return fn(*args, **kwargs)
            except RequestError as e:
                self.log(f"[WARN] API error (attempt {attempt}/{self._retry_tries}): {e}")
                if attempt == self._retry_tries:
                    raise
                time.sleep(sleep)
                sleep = min(self._retry_sleep_max, sleep * 2)

    # ---------- schedule ----------
    def is_trading_time(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)
        start = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["start_trade"]), tzinfo=tz)
        flatten = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["flatten_time"]), tzinfo=tz)
        return start <= ts_local <= flatten

    def new_entries_allowed(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)
        stop_entries = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["stop_new_entries"]), tzinfo=tz)
        return ts_local <= stop_entries

    def flatten_due(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)
        flatten = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["flatten_time"]), tzinfo=tz)
        return ts_local >= flatten

    @staticmethod
    def _parse_hhmm(s: str):
        hh, mm = s.split(":")
        return datetime.strptime(f"{hh}:{mm}", "%H:%M").time()

    # ---------- routing helpers (sandbox vs real) ----------
    def _positions_call(self):
        return self.client.sandbox.get_sandbox_positions if self.use_sandbox else self.client.operations.get_positions

    def _orders_list_call(self):
        return self.client.sandbox.get_sandbox_orders if self.use_sandbox else self.client.orders.get_orders

    def _order_post_call(self):
        return self.client.sandbox.post_sandbox_order if self.use_sandbox else self.client.orders.post_order

    def _order_cancel_call(self):
        return self.client.sandbox.cancel_sandbox_order if self.use_sandbox else self.client.orders.cancel_order

    def _order_state_call(self):
        return self.client.sandbox.get_sandbox_order_state if self.use_sandbox else self.client.orders.get_order_state

    def _operations_call(self):
        return self.client.sandbox.get_sandbox_operations if self.use_sandbox else self.client.operations.get_operations

    # ---------- accounts / sandbox ----------
    def pick_account_id(self) -> str:
        """
        In sandbox:
          - if no accounts -> open
          - optional pay-in at creation (sandbox_pay_in_rub)
        In real:
          - pick first account from users.get_accounts()
        """
        if self.use_sandbox:
            accs = self._call(self.client.sandbox.get_sandbox_accounts).accounts
            if not accs:
                self.log("[INFO] No sandbox accounts. Creating one...")
                created = self._call(self.client.sandbox.open_sandbox_account)
                account_id = created.account_id
                self.log(f"[INFO] Created sandbox account: {account_id}")

                init_rub = float(self.cfg.get("sandbox_pay_in_rub", 0.0))
                if init_rub > 0:
                    try:
                        self._call(
                            self.client.sandbox.sandbox_pay_in,
                            account_id=account_id,
                            amount=self._money_value(init_rub, self.currency),
                        )
                        self.log(f"[INFO] Sandbox pay-in: {init_rub:.2f} {self.currency}")
                    except Exception as e:
                        self.log(f"[WARN] Sandbox pay-in failed: {e}")

                return account_id

            return accs[0].id

        resp = self._call(self.client.users.get_accounts)
        if not resp.accounts:
            raise RuntimeError("Нет доступных счетов")
        return resp.accounts[0].id

    @staticmethod
    def _money_value(amount: float, currency: str):
        # keep robust conversion float -> Decimal -> Quotation -> MoneyValue
        q = decimal_to_quotation(Decimal(str(float(amount))))
        from tinkoff.invest import MoneyValue  # type: ignore
        return MoneyValue(units=q.units, nano=q.nano, currency=currency)

    # ---------- sandbox cash helpers ----------
    def get_cash_rub(self, account_id: str) -> float:
        """
        Returns available cash in self.currency (default rub).
        Uses positions call (sandbox or real).
        """
        try:
            pos = self._call(self._positions_call(), account_id=account_id)
            cash = 0.0
            for m in pos.money:
                if m.currency == self.currency:
                    cash += float(self._to_float(m))
            return float(cash)
        except Exception as e:
            self.log(f"[WARN] get_cash_rub failed: {e}")
            return 0.0

    def ensure_sandbox_cash(self, account_id: str, min_cash_rub: float):
        """
        If sandbox and cash < min_cash_rub -> top-up using sandbox_pay_in.
        """
        if not self.use_sandbox:
            return

        min_cash_rub = float(min_cash_rub)
        cash = self.get_cash_rub(account_id)
        if cash >= min_cash_rub:
            self.log(f"[INFO] Sandbox cash OK: {cash:.2f} {self.currency}")
            return

        topup = max(0.0, min_cash_rub - cash)
        try:
            self._call(
                self.client.sandbox.sandbox_pay_in,
                account_id=account_id,
                amount=self._money_value(topup, self.currency),
            )
            self.log(f"[INFO] Sandbox pay-in: +{topup:.2f} {self.currency} (cash was {cash:.2f})")
        except Exception as e:
            self.log(f"[WARN] Sandbox pay-in failed: {e}")

    # ---------- instruments ----------
    def resolve_instruments(self, tickers: List[str]) -> Dict[str, InstrumentInfo]:
        """
        Resolve MOEX shares via share_by(TICKER, class_code) to avoid futures/derivatives.
        """
        out: Dict[str, InstrumentInfo] = {}
        for t in tickers:
            try:
                r = self._call(
                    self.client.instruments.share_by,
                    id=t,
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                    class_code=self.class_code,
                )
                share = r.instrument
            except Exception as e:
                self.log(f"[WARN] share_by(TICKER) failed for {t} class={self.class_code}: {e}")
                continue

            figi = share.figi
            lot = int(share.lot)
            mpi = float(self._to_float(share.min_price_increment))

            info = InstrumentInfo(ticker=t, figi=figi, lot=lot, min_price_increment=float(mpi))
            out[t] = info
            self._figi_info[figi] = info

        return out

    def pick_tradeable_figis(self, universe_cfg: dict, max_lot_cost: float) -> List[str]:
        instruments = self.resolve_instruments(universe_cfg["tickers"])
        figis: List[str] = []

        for t, info in instruments.items():
            last_price = self.get_last_price(info.figi)
            if last_price is None:
                self.log(f"[SKIP] {t} no last price")
                continue

            last_price_f = float(last_price)
            lot_cost = last_price_f * int(info.lot)

            if lot_cost <= float(max_lot_cost):
                figis.append(info.figi)
                self.log(f"[OK] {t} {info.figi} lot={info.lot} lot_cost≈{lot_cost:.2f}")
            else:
                self.log(f"[SKIP] {t} lot_cost≈{lot_cost:.2f} > {float(max_lot_cost):.2f}")

        return figis

    # ---------- price helpers ----------
    @staticmethod
    def _round_to_step_down(price: float, step: float) -> float:
        if step <= 0:
            return float(price)
        return float(math.floor(float(price) / float(step)) * float(step))

    def _normalize_price(self, figi: str, price: float) -> float:
        info = self._figi_info.get(figi)
        if not info:
            return float(price)
        return float(self._round_to_step_down(float(price), float(info.min_price_increment)))

    # ---------- market data ----------
    def get_last_price(self, figi: str) -> Optional[float]:
        try:
            r = self._call(self.client.market_data.get_last_prices, figi=[figi])
            if not r.last_prices:
                return None

            return float(self._to_float(r.last_prices[0].price))
        except Exception:
            return None

    def get_last_candles_1m(self, figi: str, lookback_minutes: int) -> Optional[pd.DataFrame]:
        to_ = now()
        from_ = to_ - timedelta(minutes=lookback_minutes + 5)

        try:
            candles = []
            for c in self.client.get_all_candles(
                figi=figi,
                from_=from_,
                to=to_,
                interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
            ):
                candles.append(c)

            if not candles:
                return None

            df = pd.DataFrame(
                {
                    "time": [x.time for x in candles],
                    "open": [float(self._to_float(x.open)) for x in candles],
                    "high": [float(self._to_float(x.high)) for x in candles],
                    "low": [float(self._to_float(x.low)) for x in candles],
                    "close": [float(self._to_float(x.close)) for x in candles],
                    "volume": [int(x.volume) for x in candles],
                }
            )
            return df
        except RequestError as e:
            self.log(f"[WARN] candles error {figi}: {e}")
            return None

    # ---------- state sync ----------
    def sync_state(self, account_id: str, figi: str):
        """
        Updates:
          - position lots for figi
          - active order id for figi
          - entry bookkeeping
        """
        self._ensure_day_rollover()
        fs = self.state.get(figi)

        prev_lots = int(fs.position_lots)

        # Positions
        try:
            pos = self._call(self._positions_call(), account_id=account_id)
            lots = 0
            for sec in pos.securities:
                if sec.figi == figi:
                    # IMPORTANT: sometimes SDK fields can be "0" (int) or Quotation-like.
                    # We must not call .nano on int.
                    lots_f = self._to_float(getattr(sec, "balance", 0))
                    lots = int(lots_f)
                    break
            fs.position_lots = int(lots)
        except Exception as e:
            self.log(f"[WARN] get_positions failed: {e}")

        # Orders
        try:
            orders = self._call(self._orders_list_call(), account_id=account_id).orders
            active = [o for o in orders if o.figi == figi]
            fs.active_order_id = active[0].order_id if active else None
        except Exception as e:
            self.log(f"[WARN] get_orders failed: {e}")

        # Entry bookkeeping
        if prev_lots > 0 and int(fs.position_lots) == 0:
            fs.entry_price = None
            fs.entry_time = None

        if prev_lots == 0 and int(fs.position_lots) > 0:
            if fs.entry_time is None:
                fs.entry_time = now()
            if fs.entry_price is None:
                last = self.get_last_price(figi)
                if last is not None:
                    fs.entry_price = float(last)

    # ---------- orders ----------
    def cancel_active_order(self, account_id: str, figi: str):
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return

        oid = fs.active_order_id
        cuid = fs.client_order_uid or ""

        try:
            self._call(self._order_cancel_call(), account_id=account_id, order_id=oid)
            self.log(f"[CANCEL] {figi} order_id={oid}")
            self.notify(f"[CANCEL] {self._ticker_for_figi(figi) or figi} order_id={oid}", throttle_sec=0)

            self.journal_event(
                "CANCEL",
                figi,
                side="",
                lots=None,
                price=None,
                order_id=oid,
                client_uid=cuid,
                status="CANCELLED",
                reason="cancel_active_order",
            )

            fs.active_order_id = None
            fs.client_order_uid = None
        except Exception as e:
            self.log(f"[WARN] cancel_order failed: {e}")
            self.notify(f"[WARN] cancel failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)

    def place_limit_buy(self, account_id: str, figi: str, price: float, quantity_lots: int = 1) -> bool:
        fs = self.state.get(figi)

        if fs.active_order_id:
            return False
        if int(fs.position_lots) > 0:
            return False

        price_f = self._normalize_price(figi, float(price))

        # cash pre-check (prevents repeated "not enough balance" API rejects)
        info = self._figi_info.get(figi)
        lot = int(info.lot) if info else 1
        est_cost = float(price_f) * float(lot) * float(quantity_lots)
        cash = self.get_cash_rub(account_id)
        if cash > 0 and cash < est_cost * 1.01:  # small buffer
            now_ts = time.time()
            last_warn = self._last_low_cash_warn.get(figi, 0.0)
            if now_ts - last_warn >= 300:  # warn at most once per 5 minutes per figi
                self._last_low_cash_warn[figi] = now_ts
                msg = f"[SKIP] BUY {figi}: not enough cash (cash={cash:.2f} need≈{est_cost:.2f})"
                self.log(msg)
                self.notify(msg, throttle_sec=0)
            self.journal_event(
                "SKIP",
                figi,
                side="BUY",
                lots=int(quantity_lots),
                price=float(price_f),
                order_id=None,
                client_uid=None,
                status="NO_CASH",
                reason="insufficient_cash_precheck",
            )
            return False
        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(Decimal(str(price_f)))

        try:
            r = self._call(
                self._order_post_call(),
                account_id=account_id,
                figi=figi,
                quantity=int(quantity_lots),
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_BUY,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            inst = self.format_instrument(figi)
            cash = self.get_cached_cash_rub(account_id)
            self.log(
                f"[ORDER] BUY {inst} qty={int(quantity_lots)} price={price_f} | cash≈{cash:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.notify(
                f"[ORDER] BUY {inst} qty={int(quantity_lots)} price={price_f} | cash≈{cash:.2f} {self.currency.upper()}",
                throttle_sec=0,
            )

            self.journal_event(
                "SUBMIT",
                figi,
                side="BUY",
                lots=int(quantity_lots),
                price=float(price_f),
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_buy",
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order BUY failed: {e}")
            self.notify(f"[WARN] BUY submit failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)
            return False

    def place_limit_sell_to_close(self, account_id: str, figi: str, price: float) -> bool:
        fs = self.state.get(figi)
        if int(fs.position_lots) <= 0:
            return False

        if fs.active_order_id:
            self.cancel_active_order(account_id, figi)

        price_f = self._normalize_price(figi, float(price))
        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(Decimal(str(price_f)))

        try:
            r = self._call(
                self._order_post_call(),
                account_id=account_id,
                figi=figi,
                quantity=int(fs.position_lots),
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id

            inst = self.format_instrument(figi)
            cash = self.get_cached_cash_rub(account_id)
            self.log(
                f"[ORDER] SELL {inst} qty={int(fs.position_lots)} price={price_f} | cash≈{cash:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.notify(
                f"[ORDER] SELL {inst} qty={int(fs.position_lots)} price={price_f} | cash≈{cash:.2f} {self.currency.upper()}",
                throttle_sec=0,
            )

            self.journal_event(
                "SUBMIT",
                figi,
                side="SELL",
                lots=int(fs.position_lots),
                price=float(price_f),
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_sell_to_close",
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order SELL failed: {e}")
            self.notify(f"[WARN] SELL submit failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)
            return False

    # ---------- order execution polling ----------
    def poll_order_updates(self, account_id: str, figi: str):
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return

        oid = fs.active_order_id
        cuid = fs.client_order_uid or ""

        try:
            st = self._call(self._order_state_call(), account_id=account_id, order_id=oid)
        except Exception as e:
            self.log(f"[WARN] get_order_state failed {figi}: {e}")
            return

        status = str(getattr(st, "execution_report_status", ""))
        lots_requested = int(getattr(st, "lots_requested", 0) or 0)
        lots_executed = int(getattr(st, "lots_executed", 0) or 0)
        direction = str(getattr(st, "direction", ""))

        avg_price = None
        ap = getattr(st, "average_position_price", None)
        if ap is not None:
            try:
                avg_price = float(self._to_float(ap))
            except Exception:
                avg_price = None

        side = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else "")

        # partial fill
        if lots_executed > 0 and lots_requested > 0 and lots_executed < lots_requested:
            self.journal_event(
                "PARTIAL_FILL",
                figi,
                side=side,
                lots=lots_executed,
                price=avg_price,
                order_id=oid,
                client_uid=cuid,
                status=status,
                reason="partial_fill",
                meta={"lots_requested": lots_requested},
            )

        final_statuses = {
            "EXECUTION_REPORT_STATUS_FILL",
            "EXECUTION_REPORT_STATUS_REJECTED",
            "EXECUTION_REPORT_STATUS_CANCELLED",
        }

        if status in final_statuses:
            if status == "EXECUTION_REPORT_STATUS_FILL":
                self.journal_event(
                    "FILL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="filled",
                )

                # Count "trades" on ENTRY fills (BUY) rather than submits.
                if side == "BUY":
                    self.state.trades_today += 1

                self.notify(
                    f"[FILL] {side} {self._ticker_for_figi(figi) or figi} lots={lots_executed} price={avg_price}",
                    throttle_sec=0,
                )

                if side == "BUY":
                    if fs.entry_time is None:
                        fs.entry_time = now()
                    if fs.entry_price is None and avg_price is not None:
                        fs.entry_price = float(avg_price)
                elif side == "SELL":
                    fs.entry_price = None
                    fs.entry_time = None

            elif status == "EXECUTION_REPORT_STATUS_CANCELLED":
                self.journal_event(
                    "CANCEL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="cancelled_by_api",
                )
                self.notify(f"[CANCELLED] {self._ticker_for_figi(figi) or figi}", throttle_sec=0)

            elif status == "EXECUTION_REPORT_STATUS_REJECTED":
                self.journal_event(
                    "REJECT",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="rejected",
                )
                self.notify(f"[REJECT] {self._ticker_for_figi(figi) or figi} | status={status}", throttle_sec=60)

            fs.active_order_id = None
            fs.client_order_uid = None

    # ---------- flatten ----------
    def flatten_if_needed(self, account_id: str, schedule_cfg: dict):
        ts = now()
        if not self.flatten_due(ts, schedule_cfg):
            return

        for figi in list(self.state.figi.keys()):
            fs = self.state.get(figi)

            if fs.active_order_id:
                self.cancel_active_order(account_id, figi)

            if int(fs.position_lots) > 0:
                last = self.get_last_price(figi)
                if last is None:
                    continue
                self.place_limit_sell_to_close(account_id, figi, price=float(last))

    # ---------- day metric ----------
    def calc_day_cashflow(self, account_id: str) -> float:
        """
        Protective day metric: sum of operation payments for today in selected currency.
        Uses sandbox operations in sandbox mode.
        """
        try:
            tz = ZoneInfo("Europe/Moscow")
            today_local = datetime.now(tz=tz).date()

            from_local = datetime.combine(today_local, datetime.min.time(), tzinfo=tz)
            to_local = datetime.combine(today_local, datetime.max.time(), tzinfo=tz)

            from_utc = from_local.astimezone(ZoneInfo("UTC"))
            to_utc = to_local.astimezone(ZoneInfo("UTC"))

            ops = self._call(self._operations_call(), account_id=account_id, from_=from_utc, to=to_utc)

            total = 0.0
            for op in ops.operations:
                if op.payment.currency == self.currency:
                    total += float(self._to_float(op.payment))

            return float(total)
        except Exception as e:
            self.log(f"[WARN] calc_day_cashflow failed: {e}")
            return 0.0
