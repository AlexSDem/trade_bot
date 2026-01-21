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

        # NEW: guardrails to reduce "signals but no fills"
        # If last is only slightly below buy_level -> it's a shallow signal (often noisy).
        self.min_edge_atr = float(cfg.get("min_edge_atr", 0.05))  # edge in ATR units (0.05 ATR by default)

        # If last rebounds too far above buy_level, don't chase (skip)
        self.max_rebound_atr = float(cfg.get("max_rebound_atr", 0.25))  # 0.25 ATR

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

        Returns dict like:
          - action: BUY/SELL/HOLD
          - price: last close (for reporting)
          - limit_price: recommended LIMIT price (for BUY/SELL)
          - reason: string
        """
        df = candles.tail(self.lookback).copy()
        last = float(df["close"].iloc[-1])

        atr = self._atr(df, 14)
        if not np.isfinite(atr) or atr <= 0:
            return {"action": "HOLD", "price": last, "reason": "ATR not ready"}

        vwap = self._vwap(df)
        fs = state.get(figi)
        has_pos = int(getattr(fs, "position_lots", 0) or 0) > 0
        has_active_order = bool(getattr(fs, "active_order_id", None))

        # If an order is already working - do not generate new entry/exit signals.
        # Order management happens in broker/main (TTL, polling, etc.)
        if has_active_order:
            return {"action": "HOLD", "price": last, "reason": "active_order_wait"}

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
                        "limit_price": last,
                        "reason": f"time_stop {age}",
                    }

            if last >= take_level:
                return {
                    "action": "SELL",
                    "price": last,
                    "limit_price": last,
                    "reason": f"take_profit last>={take_level:.4f}",
                }

            if last <= stop_level:
                return {
                    "action": "SELL",
                    "price": last,
                    "limit_price": last,
                    "reason": f"stop_loss last<={stop_level:.4f}",
                }

            return {"action": "HOLD", "price": last, "reason": "in_position"}

        # =========================
        # ENTRY LOGIC (BUY)
        # =========================
        buy_level = float(vwap - self.k * atr)

        # Edge in ATR units (how deep below buy_level are we)
        edge_atr = float((buy_level - last) / atr)

        # If signal is too shallow -> skip (noise)
        if edge_atr < self.min_edge_atr:
            return {"action": "HOLD", "price": last, "reason": f"no_edge edge_atr={edge_atr:.3f}"}

        # Classic mean reversion: last below buy_level
        if last < buy_level:
            # Recommend placing LIMIT at buy_level (not at last),
            # so you don't accidentally place too far below the market and never get filled.
            limit_price = buy_level

            # If price already rebounded too far above buy_level, don't chase
            rebound_atr = float((last - buy_level) / atr)  # negative here usually, keep for completeness
            if rebound_atr > self.max_rebound_atr:
                return {"action": "HOLD", "price": last, "reason": f"skip_chase rebound_atr={rebound_atr:.3f}"}

            return {
                "action": "BUY",
                "price": last,
                "limit_price": float(limit_price),
                "reason": f"mean_reversion last<{buy_level:.4f} edge_atr={edge_atr:.3f}",
            }

        return {"action": "HOLD", "price": last, "reason": "no_edge"}
