class RiskManager:
    def __init__(self, cfg: dict):
        self.max_day_loss_rub = float(cfg.get("max_day_loss_rub", 100.0))
        self.max_trades_per_day = int(cfg.get("max_trades_per_day", 3))
        self._day_pnl = 0.0
        self._locked = False

    def update_day_pnl(self, pnl_rub: float):
        self._day_pnl = pnl_rub
        if pnl_rub <= -self.max_day_loss_rub:
            self._locked = True

    def day_locked(self) -> bool:
        return self._locked

    def allow_new_trade(self, state, account_id: str, figi: str) -> bool:
        # 1) максимум 1 позиция
        if state.has_open_position(figi):
            return False
        # 2) если уже заблокированы по дню
        if self._locked:
            return False
        # 3) лимит по количеству сделок (счётчик веди в state)
        if state.trades_today >= self.max_trades_per_day:
            return False
        return True
