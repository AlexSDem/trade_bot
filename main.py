import os
import time
import yaml
from datetime import datetime, timezone

from tinkoff.invest import Client
from tinkoff.invest.utils import now

from strategy import Strategy
from risk import RiskManager
from broker import Broker
from telegram_notifier import notifier_from_env
from report_day import load_trades, build_report


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_token() -> str:
    token = os.environ.get("INVEST_TOKEN")
    if not token:
        raise RuntimeError(
            "Не задан INVEST_TOKEN.\n"
            "В PowerShell выполни:\n"
            '  setx INVEST_TOKEN "ТВОЙ_ТОКЕН"\n'
            "Затем открой новое окно PowerShell и запусти снова."
        )
    return token


def _send_daily_report(cfg: dict, broker: Broker, notifier, report_sent_for_day: str | None) -> str | None:
    """Returns updated report_sent_for_day."""
    day_key = datetime.now(timezone.utc).date().isoformat()
    if report_sent_for_day == day_key:
        return report_sent_for_day

    try:
        df = load_trades(cfg["broker"].get("trades_csv", "logs/trades.csv"))
        report = build_report(df, datetime.now(timezone.utc).date())
        broker.log(report)
        notifier.send(report, throttle_sec=0)
        return day_key
    except Exception as e:
        broker.log(f"[WARN] Daily report generation failed: {e}")
        return report_sent_for_day


def main():
    cfg = load_config()
    token = get_token()

    notifier = notifier_from_env(enabled=bool(cfg.get("telegram", {}).get("enabled", True)))

    strategy = Strategy(cfg["strategy"])
    risk = RiskManager(cfg["risk"])

    sleep_sec = float(cfg.get("runtime", {}).get("sleep_sec", 55))
    error_sleep_sec = float(cfg.get("runtime", {}).get("error_sleep_sec", 10))
    heartbeat_sec = float(cfg.get("runtime", {}).get("heartbeat_sec", 300))

    # NEW: TTL for limit orders (seconds)
    order_ttl_sec = int(cfg.get("runtime", {}).get("order_ttl_sec", 300))

    with Client(token) as client:
        broker = Broker(client, cfg["broker"], notifier=notifier)

        account_id = broker.pick_account_id()
        broker.log(f"[INFO] Account: {account_id} (sandbox={cfg['broker'].get('use_sandbox', True)})")

        notifier.send(
            f"trade_bot started\naccount={account_id}\nsandbox={cfg['broker'].get('use_sandbox', True)}",
            throttle_sec=0,
        )

        # ensure sandbox cash
        if cfg["broker"].get("use_sandbox", True):
            min_cash = float(cfg["broker"].get("min_sandbox_cash_rub", 12000))
            broker.ensure_sandbox_cash(account_id, min_cash_rub=min_cash)

        figis = broker.pick_tradeable_figis(cfg["universe"], max_lot_cost=cfg["risk"]["max_lot_cost_rub"])
        broker.log(f"[INFO] Tradeable FIGIs: {figis}")

        if not figis:
            broker.log("[ERROR] Нет подходящих инструментов под max_lot_cost_rub. Увеличь лимит или измени tickers.")
            return

        last_hb = 0.0
        consecutive_errors = 0
        report_sent_for_day: str | None = None

        while True:
            try:
                ts = now()

                # Heartbeat
                if time.time() - last_hb >= heartbeat_sec:
                    broker.log(f"[HB] alive | utc={ts.isoformat()}")
                    last_hb = time.time()

                # Outside trading window: only flatten + daily report once
                if not broker.is_trading_time(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    if broker.flatten_due(ts, cfg["schedule"]):
                        report_sent_for_day = _send_daily_report(cfg, broker, notifier, report_sent_for_day)

                    time.sleep(min(10, sleep_sec))
                    continue

                # Flatten time
                if broker.flatten_due(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    report_sent_for_day = _send_daily_report(cfg, broker, notifier, report_sent_for_day)
                    time.sleep(min(10, sleep_sec))
                    continue

                # Day lock
                if risk.day_locked():
                    time.sleep(30)
                    continue

                entries_allowed = broker.new_entries_allowed(ts, cfg["schedule"])

                # 1x per loop: account snapshot (positions + orders)
                broker.refresh_account_snapshot(account_id, figis)

                for figi in figis:
                    # 1) order state updates
                    broker.poll_order_updates(account_id, figi)

                    # 2) expire stale orders (TTL) to avoid "stuck all day" behavior
                    # if expired -> skip signals this loop for this figi
                    if order_ttl_sec > 0:
                        expired = broker.expire_stale_orders(account_id, figi, ttl_sec=order_ttl_sec)
                        if expired:
                            continue

                    # 3) candles
                    candles = broker.get_last_candles_1m(figi, lookback_minutes=cfg["strategy"]["lookback_minutes"])
                    if candles is None or len(candles) < 30:
                        continue

                    # 4) signal
                    signal = strategy.make_signal(figi, candles, broker.state)
                    action = signal.get("action", "HOLD")

                    # pick price for logging and limit placement
                    last_price = signal.get("price")
                    limit_price = signal.get("limit_price", last_price)  # NEW: strategy can suggest limit_price
                    reason = signal.get("reason", "")

                    # journal signals
                    if action in ("BUY", "SELL"):
                        inst = broker.format_instrument(figi)
                        cash = broker.get_cached_cash_rub(account_id)
                        free_cash = broker.get_free_cash_rub_estimate(account_id)

                        lp_txt = f" limit={float(limit_price):.4f}" if limit_price is not None else ""
                        broker.log(
                            f"[SIGNAL] {action} {inst} last={last_price}{lp_txt} "
                            f"| cash≈{cash:.2f} free≈{free_cash:.2f} {cfg['broker'].get('currency','rub').upper()} | {reason}"
                        )

                        broker.journal_event(
                            "SIGNAL",
                            figi,
                            side=action,
                            lots=1,
                            price=float(last_price) if last_price is not None else None,
                            reason=reason,
                            meta={"limit_price": float(limit_price) if limit_price is not None else None},
                        )

                    # 5) execution
                    if action == "BUY":
                        if not entries_allowed:
                            continue
                        if not risk.allow_new_trade(broker.state, account_id, figi):
                            continue

                        # NEW: place buy using limit_price (recommended) instead of last
                        if limit_price is None:
                            continue
                        broker.place_limit_buy(account_id, figi, float(limit_price))

                    elif action == "SELL":
                        if limit_price is None:
                            continue
                        broker.place_limit_sell_to_close(account_id, figi, float(limit_price))

                # day safety metric
                day_metric = broker.calc_day_cashflow(account_id)
                risk.update_day_pnl(day_metric)

                time.sleep(sleep_sec)
                consecutive_errors = 0

            except KeyboardInterrupt:
                broker.log("[INFO] Stopped by user (Ctrl+C). Trying to flatten...")
                notifier.send("trade_bot stopped by user (Ctrl+C). Flattening...", throttle_sec=0)
                try:
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                except Exception as e:
                    broker.log(f"[WARN] Flatten on exit failed: {e}")
                break

            except Exception as e:
                try:
                    broker.log(f"[ERROR] Main loop error: {e}")
                except Exception:
                    print("Main loop error:", e)

                consecutive_errors += 1
                notifier.send(f"[ERROR] Main loop error: {e}", throttle_sec=120)

                if consecutive_errors >= int(cfg.get("runtime", {}).get("max_consecutive_errors", 8)):
                    broker.log("[ERROR] Too many consecutive errors. Stopping bot.")
                    notifier.send("[FATAL] Too many consecutive errors. Stopping bot.", throttle_sec=0)
                    break
                time.sleep(error_sleep_sec)


if __name__ == "__main__":
    main()
