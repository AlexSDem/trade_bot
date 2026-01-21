from dataclasses import dataclass, field
from typing import Optional, Dict
from datetime import datetime, timezone, timedelta


@dataclass
class FigiState:
    # Биржевой order_id (ответ API)
    active_order_id: Optional[str] = None

    # Наш idempotency key (client uid)
    client_order_uid: Optional[str] = None

    # Лоты позиции
    position_lots: int = 0

    # Для стратегии (тейк/стоп/тайм-стоп)
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None

    # --- NEW: order bookkeeping (for TTL / risk / diagnostics) ---
    # "BUY" / "SELL" / None
    order_side: Optional[str] = None

    # UTC timestamp when order was submitted (best effort)
    order_placed_ts: Optional[datetime] = None

    # Optional: per-figi cooldown to avoid spamming after rejects / low cash
    cooldown_until: Optional[datetime] = None


@dataclass
class BotState:
    figi: Dict[str, FigiState] = field(default_factory=dict)
    trades_today: int = 0
    current_day: Optional[str] = None  # YYYY-MM-DD (UTC)

    def get(self, figi: str) -> FigiState:
        if figi not in self.figi:
            self.figi[figi] = FigiState()
        return self.figi[figi]

    def has_open_position(self, figi: str) -> bool:
        fs = self.figi.get(figi)
        return bool(fs and int(fs.position_lots) > 0)

    def has_active_order(self, figi: str) -> bool:
        fs = self.figi.get(figi)
        return bool(fs and fs.active_order_id)

    def active_orders_count(self) -> int:
        return sum(1 for fs in self.figi.values() if fs.active_order_id)

    def open_positions_count(self) -> int:
        return sum(1 for fs in self.figi.values() if int(fs.position_lots) > 0)

    def pending_buys_count(self) -> int:
        """
        Counts pending BUY entries:
        - active order exists
        - no open position
        - order_side == BUY OR (order_side is None -> treat as BUY conservatively)
        """
        cnt = 0
        for fs in self.figi.values():
            if fs.active_order_id and int(fs.position_lots) == 0:
                if (fs.order_side is None) or (str(fs.order_side).upper() == "BUY"):
                    cnt += 1
        return cnt

    def clear_entry(self, figi: str):
        fs = self.get(figi)
        fs.entry_price = None
        fs.entry_time = None

    def clear_order(self, figi: str):
        fs = self.get(figi)
        fs.active_order_id = None
        fs.client_order_uid = None
        fs.order_side = None
        fs.order_placed_ts = None

    def set_cooldown(self, figi: str, seconds: int):
        fs = self.get(figi)
        fs.cooldown_until = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)  # make sure aware
        fs.cooldown_until = fs.cooldown_until + timedelta(seconds=int(seconds))  # type: ignore

    def in_cooldown(self, figi: str) -> bool:
        fs = self.figi.get(figi)
        if not fs or not fs.cooldown_until:
            return False
        now_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
        return now_utc < fs.cooldown_until

    def reset_day(self, day_key: str):
        self.current_day = day_key
        self.trades_today = 0
        # entry_* не трогаем — это состояние позиции, не дня

    def touch_day(self, day_key: str):
        """
        Если день изменился — сбрасываем дневные счётчики.
        """
        if self.current_day != day_key:
            self.reset_day(day_key)
