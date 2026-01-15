from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict


@dataclass
class FigiState:
    active_order_id: Optional[str] = None       # биржевой order_id (ответ API)
    client_order_uid: Optional[str] = None      # наш idempotency key
    position_lots: int = 0

    # Для стратегии (тейк/стоп/тайм-стоп)
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None


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
        return bool(fs and fs.position_lots > 0)

    def reset_day(self, day_key: str):
        self.current_day = day_key
        self.trades_today = 0
        # entry_* не трогаем — это состояние позиции, не дня
