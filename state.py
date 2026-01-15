from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict

@dataclass
class FigiState:
    active_order_id: Optional[str] = None       # биржевой order_id (ответ API)
    client_order_uid: Optional[str] = None      # наш idempotency key
    position_lots: int = 0
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None

@dataclass
class BotState:
    figi: Dict[str, FigiState] = field(default_factory=dict)
    trades_today: int = 0
    day_locked: bool = False
    current_day: Optional[str] = None  # YYYY-MM-DD
