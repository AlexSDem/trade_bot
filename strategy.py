import numpy as np
import pandas as pd
from datetime import timedelta


class Strategy:
    def __init__(self, cfg: dict):
        self.k = float(cfg.get("k_atr", 1.2))
        self.take_pct = float(cfg.get("take_profit_pct", 0.004))
        self.stop_pct = float(cfg.get("stop_loss_pct", 0.006))
        self.lookback = int(cfg.get("lookback_minutes", 180))
        self.time_stop_minutes = int(cfg.get("time_stop_minutes", 45))

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> float:
        high = df["high"].values
        low = df["low"].values
        close = df["close"].values
        prev_close = np.r_[close[0], close[:-1]]
        tr = np.maximum(
            high - low,
            np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
        )
        if len(tr) < n + 1:
            return float("nan")
        return float(pd.Series(tr).rolling(n).mean().iloc[-1])

    @staticmethod
    def _vwap(df: pd.DataFrame) -> float:
        pv = (df["close"] * df["volume"]).sum()
        vv = df["volume"].sum()
        if vv <= 0:
            return float(df["close"].iloc[-1])
        return float(pv / vv)

    def make_signal(self, figi: str, candles: pd.DataFrame, state) -> dict:
        """
        candles columns: time, open, high, low, close, volume
        state: BotState
        """
        df = candles.tail(self.lookback).copy()
        last = float(df["close"].iloc[-1])

        atr = self._atr(df, 14)
        if not np.isfinite(atr) or atr <= 0:
            return {"action": "HOLD", "price": last, "reason": "ATR not ready"}

        vwap = self._vwap(df)
        fs = state.get(figi)
        has_pos = fs.position_lots > 0

        # =========================
        # EXIT LOGIC (SELL)
        # =========================
        if has_pos:
            if fs.entry_price is None:
                fs.entry_price = last
                fs.entry_time = df["time"].iloc[-1]

            entry = float(fs.entry_price)
            take_level = max(entry * (1 + self.take_pct), vwap)
            stop_level = entry * (1 - self.stop_pct)

            if fs.entry_time is not None:
                age = df["time"].iloc[-1] - fs.entry_time
                if age >= timedelta(minutes=self.time_stop_minutes):
                    return {
                        "action": "SELL",
                        "price": last,
                        "reason": f"time_stop {age}",
                    }

            if last >= take_level:
                return {
                    "action": "SELL",
                    "price": last,
                    "reason": f"take_profit last>={take_level:.4f}",
                }

            if last <= stop_level:
                return {
                    "action": "SELL",
                    "price": last,
                    "reason": f"stop_loss last<={stop_level:.4f}",
                }

            return {"action": "HOLD", "price": last, "reason": "in_position"}

        # =========================
        # ENTRY LOGIC (BUY)
        # =========================
        buy_level = vwap - self.k * atr
        if last < buy_level:
            return {
                "action": "BUY",
                "price": last,
                "reason": f"mean_reversion last<{buy_level:.4f}",
            }

        return {"action": "HOLD", "price": last, "reason": "no_edge"}
