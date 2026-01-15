import os
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Tuple

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

from state import BotState, FigiState


@dataclass
class InstrumentInfo:
    ticker: str
    figi: str
    lot: int
    min_price_increment: float


class Broker:
    def __init__(self, client: Client, cfg: dict):
        self.client = client
        self.cfg = cfg
        self.state = BotState()
        self.logger = logging.getLogger("bot")

        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            filename=cfg.get("log_file", "logs/bot.log"),
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )

        self.currency = cfg.get("currency", "rub")

    # ---------- helpers ----------
    def log(self, msg: str):
        self.logger.info(msg)
        print(msg)

    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo("UTC")).date().isoformat()

    def _ensure_day_rollover(self):
        today = self._today_key()
        if self.state.current_day != today:
            self.state.current_day = today
            self.state.trades_today = 0
            self.state.day_locked = False

    def pick_account_id(self) -> str:
        # в sandbox и в бою метод одинаковый: users.get_accounts()
        resp = self.client.users.get_accounts()
        if not resp.accounts:
            raise RuntimeError("Нет доступных счетов")
        # берём первый — для MVP ок
        return resp.accounts[0].id

    # ---------- schedule ----------
    def is_trading_time(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)

        start = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["start_trade"]), tzinfo=tz)
        stop_entries = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["stop_new_entries"]), tzinfo=tz)
        flatten = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["flatten_time"]), tzinfo=tz)

        # мы "в окне", если после старта и до flatten
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

    # ---------- instruments ----------
    def resolve_instruments(self, tickers: List[str]) -> Dict[str, InstrumentInfo]:
        """
        Возвращает тикер -> InstrumentInfo (FIGI, lot, min_price_increment)
        Используем find_instrument (официальный метод поиска). :contentReference[oaicite:4]{index=4}
        """
        out: Dict[str, InstrumentInfo] = {}
        for t in tickers:
            try:
                r = self.client.instruments.find_instrument(query=t)
            except RequestError as e:
                self.log(f"[WARN] find_instrument failed for {t}: {e}")
                continue

            # Берём первый подходящий share (акцию), где доступна торговля по API
            candidate = None
            for inst in r.instruments:
                # в ответе есть type и флаги доступности; точные поля зависят от версии схем,
                # поэтому делаем мягкую проверку через getattr
                if getattr(inst, "instrument_type", "").upper() == "INSTRUMENT_TYPE_SHARE":
                    candidate = inst
                    break
            if candidate is None and r.instruments:
                candidate = r.instruments[0]

            if candidate is None:
                continue

            figi = candidate.figi
            # Получаем детальную инфу по FIGI (лотность/шаг цены)
            share = self.client.instruments.share_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI).instrument
            lot = int(share.lot)
            mpi = float(quotation_to_decimal(share.min_price_increment))

            out[t] = InstrumentInfo(ticker=t, figi=figi, lot=lot, min_price_increment=mpi)

        return out

    def pick_tradeable_figis(self, universe_cfg: dict, max_lot_cost: float) -> List[str]:
        """
        Фильтруем инструменты под депозит:
        берём только те, где стоимость 1 лота <= max_lot_cost.
        """
        instruments = self.resolve_instruments(universe_cfg["tickers"])
        figis: List[str] = []

        for t, info in instruments.items():
            last_price = self.get_last_price(info.figi)
            if last_price is None:
                continue

            lot_cost = last_price * info.lot
            if lot_cost <= max_lot_cost:
                figis.append(info.figi)
                self.log(f"[OK] {t} {info.figi} lot={info.lot} lot_cost≈{lot_cost:.2f}")
            else:
                self.log(f"[SKIP] {t} lot_cost≈{lot_cost:.2f} > {max_lot_cost:.2f}")

        return figis

    # ---------- market data ----------
    def get_last_price(self, figi: str) -> Optional[float]:
        try:
            r = self.client.market_data.get_last_prices(figi=[figi])
            if not r.last_prices:
                return None
            return float(quotation_to_decimal(r.last_prices[0].price))
        except RequestError:
            return None

    def get_last_candles_1m(self, figi: str, lookback_minutes: int) -> Optional[pd.DataFrame]:
        """
        Пулл минутных свечей.
        В SDK есть паттерн "get_all_candles" (см. официальные примеры). :contentReference[oaicite:5]{index=5}
        Чтобы не тащить весь день, берём окно lookback_minutes.
        """
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
                    "open": [float(quotation_to_decimal(x.open)) for x in candles],
                    "high": [float(quotation_to_decimal(x.high)) for x in candles],
                    "low":  [float(quotation_to_decimal(x.low)) for x in candles],
                    "close":[float(quotation_to_decimal(x.close)) for x in candles],
                    "volume":[int(x.volume) for x in candles],
                }
            )
            return df
        except RequestError as e:
            self.log(f"[WARN] candles error {figi}: {e}")
            return None

    # ---------- orders / positions ----------
    def sync_state(self, account_id: str, figi: str):
        """
        Обновляем:
        - активные заявки по figi
        - позицию по figi
        """
        self._ensure_day_rollover()
        if figi not in self.state.figi:
            self.state.figi[figi] = FigiState()

        fs = self.state.figi[figi]

        # Позиции
        try:
            pos = self.client.operations.get_positions(account_id=account_id)
            lots = 0
            for sec in pos.securities:
                if sec.figi == figi:
                    lots = int(sec.balance)
                    break
            fs.position_lots = lots
        except RequestError as e:
            self.log(f"[WARN] get_positions failed: {e}")

        # Активные заявки
        try:
            orders = self.client.orders.get_orders(account_id=account_id).orders
            active = [o for o in orders if o.figi == figi]
            if not active:
                fs.active_order_id = None
            else:
                # для MVP берём первую
                fs.active_order_id = active[0].order_id
        except RequestError as e:
            self.log(f"[WARN] get_orders failed: {e}")

    def has_open_position(self, figi: str) -> bool:
        fs = self.state.figi.get(figi)
        return bool(fs and fs.position_lots > 0)

    def has_active_order(self, figi: str) -> bool:
        fs = self.state.figi.get(figi)
        return bool(fs and fs.active_order_id)

    def cancel_active_order(self, account_id: str, figi: str):
        fs = self.state.figi.get(figi)
        if not fs or not fs.active_order_id:
            return
        try:
            self.client.orders.cancel_order(account_id=account_id, order_id=fs.active_order_id)
            self.log(f"[CANCEL] {figi} order_id={fs.active_order_id}")
            fs.active_order_id = None
            fs.client_order_uid = None
        except RequestError as e:
            self.log(f"[WARN] cancel_order failed: {e}")

    def place_limit_buy(self, account_id: str, figi: str, price: float, quantity_lots: int = 1) -> bool:
        """
        Выставляем лимитку на покупку.
        Обязательно используем order_id как ключ идемпотентности (до 36 символов). :contentReference[oaicite:6]{index=6}
        """
        if self.state.day_locked:
            return False

        if self.has_active_order(figi):
            return False  # не плодим дубли

        if self.has_open_position(figi):
            return False  # уже в позиции

        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(price)

        try:
            r = self.client.orders.post_order(
                account_id=account_id,
                figi=figi,
                quantity=quantity_lots,
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_BUY,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )
            fs = self.state.figi[figi]
            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            self.state.trades_today += 1
            self.log(f"[ORDER] BUY {figi} qty={quantity_lots} price={price} (client_uid={client_uid})")
            return True
        except RequestError as e:
            self.log(f"[WARN] post_order BUY failed: {e}")
            return False

    def place_limit_sell_to_close(self, account_id: str, figi: str, price: float) -> bool:
        """
        Лимитка на закрытие позиции (продажа).
        """
        if self.has_active_order(figi):
            # если уже висит заявка — сначала отменим, чтобы не конфликтовать
            self.cancel_active_order(account_id, figi)

        fs = self.state.figi.get(figi)
        if not fs or fs.position_lots <= 0:
            return False

        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(price)

        try:
            r = self.client.orders.post_order(
                account_id=account_id,
                figi=figi,
                quantity=fs.position_lots,
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )
            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            self.log(f"[ORDER] SELL {figi} qty={fs.position_lots} price={price} (client_uid={client_uid})")
            return True
        except RequestError as e:
            self.log(f"[WARN] post_order SELL failed: {e}")
            return False

    # ---------- flatten / pnl ----------
    def flatten_if_needed(self, account_id: str, schedule_cfg: dict):
        """
        В конце дня:
        - отменить заявки
        - закрыть позицию лимиткой близко к last_price (для MVP)
        """
        ts = now()
        if not self.flatten_due(ts, schedule_cfg):
            return

        for figi, fs in self.state.figi.items():
            # отменяем активные заявки
            if fs.active_order_id:
                self.cancel_active_order(account_id, figi)

            # закрываем позицию
            if fs.position_lots > 0:
                last = self.get_last_price(figi)
                if last is None:
                    continue
                # MVP: ставим по last (в реальности лучше учитывать тик/стакан)
                self.place_limit_sell_to_close(account_id, figi, price=last)

    def calc_day_pnl(self, account_id: str) -> float:
        """
        Для MVP берём упрощённо: суммарный результат операций за сегодня.
        (Можно усложнить позже, но сейчас главное — дневной стоп.)
        """
        try:
            tz = ZoneInfo("Europe/Moscow")
            today_local = datetime.now(tz=tz).date()
            from_local = datetime.combine(today_local, datetime.min.time(), tzinfo=tz)
            to_local = datetime.combine(today_local, datetime.max.time(), tzinfo=tz)
            # API ожидает UTC timestamps
            from_utc = from_local.astimezone(ZoneInfo("UTC"))
            to_utc = to_local.astimezone(ZoneInfo("UTC"))

            ops = self.client.operations.get_operations(account_id=account_id, from_=from_utc, to=to_utc)
            pnl = 0.0
            for op in ops.operations:
                # у операций есть "payment" (MoneyValue), суммируем рубли
                if op.payment.currency == self.currency:
                    pnl += float(quotation_to_decimal(op.payment))
            return pnl
        except RequestError:
            return 0.0
