import os
import time
import yaml
from datetime import datetime, timezone

import pandas as pd

from tinkoff.invest import Client
from tinkoff.invest.utils import now

from strategy import Strategy
from risk import RiskManager
from broker import Broker

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()
    token = os.environ["INVEST_TOKEN"]

    strategy = Strategy(cfg["strategy"])
    risk = RiskManager(cfg["risk"])

    with Client(token) as client:
        broker = Broker(client, cfg["broker"])

        # 1) выбрать счет
        account_id = broker.pick_account_id()
        print("Account:", account_id)

        # 2) выбрать инструменты, которые подходят под 10к (по цене лота)
        figis = broker.pick_tradeable_figis(cfg["universe"], max_lot_cost=cfg["risk"]["max_lot_cost_rub"])
        print("Tradeable:", figis)

        # 3) основной цикл: раз в минуту (для MVP хватит)
        while True:
            ts = now()
            if not broker.is_trading_time(ts, cfg["schedule"]):
                broker.flatten_if_needed(account_id)  # закрыть позицию под конец дня
                time.sleep(10)
                continue

            if risk.day_locked():
                time.sleep(30)
                continue

            # получить последние N минутных свечей (пуллом для простоты MVP)
            for figi in figis:
                candles = broker.get_last_candles_1m(figi, lookback_minutes=cfg["strategy"]["lookback_minutes"])
                if candles is None or len(candles) < 30:
                    continue

                signal = strategy.make_signal(candles)
                # signal: {"action": "BUY"/"SELL"/"HOLD", "price": float, "reason": str}

                # обновить состояние по заявкам/позициям
                broker.sync_state(account_id, figi)

                # риск-фильтры
                if not risk.allow_new_trade(broker.state, account_id, figi):
                    continue

                # исполнение: только BUY/HOLD, так как только лонг
                if signal["action"] == "BUY":
                    ok = broker.place_limit_buy(account_id, figi, signal["price"])
                    if ok:
                        broker.log(f"BUY {figi} @ {signal['price']} | {signal['reason']}")
                elif signal["action"] == "SELL":
                    # это будет выход из позиции (тейк/стоп/тайм-стоп)
                    ok = broker.place_limit_sell_to_close(account_id, figi, signal["price"])
                    if ok:
                        broker.log(f"SELL {figi} @ {signal['price']} | {signal['reason']}")

            # фиксируем дневной PnL и блокируемся при лимите
            pnl = broker.calc_day_pnl(account_id)
            risk.update_day_pnl(pnl)

            time.sleep(55)

if __name__ == "__main__":
    main()
