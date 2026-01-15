class RiskManager:
    def __init__(self, cfg: dict):
        self.max_day_loss_rub = float(cfg.get("max_day_loss_rub", 100.0))
        self.max_trades_per_day = int(cfg.get("max_trades_per_day", 3))
        self.max_positions = int(cfg.get("max_positions", 1))
        self.max_active_orders_per_figi = int(cfg.get("max_active_orders_per_figi", 1))

        self._day_metric = 0.0
        self._locked = False

    def update_day_pnl(self, day_metric_rub: float):
        """
        В main.py сюда приходит защитный day metric (cashflow/PnL proxy).
        Если он ушёл ниже -max_day_loss_rub -> блокируем торговлю до завтра.
        """
        self._day_metric = day_metric_rub
        if day_metric_rub <= -self.max_day_loss_rub:
            self._locked = True

    def lock_day(self):
        self._locked = True

    def day_locked(self) -> bool:
        return self._locked

    def allow_new_trade(self, state, account_id: str, figi: str) -> bool:
        """
        Проверки именно на ОТКРЫТИЕ новой позиции (BUY).
        """
        # 1) дневной лок
        if self._locked:
            return False

        # 2) лимит сделок в день
        if state.trades_today >= self.max_trades_per_day:
            return False

        # 3) уже есть позиция по этому figi
        if state.has_open_position(figi):
            return False

        # 4) общий лимит одновременно открытых позиций
        open_positions = sum(1 for f, fs in state.figi.items() if fs.position_lots > 0)
        if open_positions >= self.max_positions:
            return False

        # 5) если у фигі уже висит активная заявка — не ставим новую
        fs = state.figi.get(figi)
        if fs and fs.active_order_id and self.max_active_orders_per_figi <= 1:
            return False

        return True
