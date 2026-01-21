class RiskManager:
    def __init__(self, cfg: dict):
        self.max_day_loss_rub = float(cfg.get("max_day_loss_rub", 100.0))
        self.max_trades_per_day = int(cfg.get("max_trades_per_day", 3))
        self.max_positions = int(cfg.get("max_positions", 1))

        # per-figi constraint (legacy)
        self.max_active_orders_per_figi = int(cfg.get("max_active_orders_per_figi", 1))

        # NEW: portfolio-level limits on pending orders
        # If not specified -> behave conservatively like max_positions
        self.max_pending_buys_total = int(cfg.get("max_pending_buys_total", self.max_positions))
        self.max_active_orders_total = int(cfg.get("max_active_orders_total", self.max_positions))

        self._day_metric = 0.0
        self._locked = False

    def update_day_pnl(self, day_metric_rub: float):
        """
        В main.py сюда приходит защитный day metric (cashflow/PnL proxy).
        Если он ушёл ниже -max_day_loss_rub -> блокируем торговлю до завтра.
        """
        self._day_metric = float(day_metric_rub)
        if self._day_metric <= -float(self.max_day_loss_rub):
            self._locked = True

    def lock_day(self):
        self._locked = True

    def day_locked(self) -> bool:
        return bool(self._locked)

    @staticmethod
    def _count_open_positions(state) -> int:
        return sum(1 for _, fs in state.figi.items() if int(getattr(fs, "position_lots", 0) or 0) > 0)

    @staticmethod
    def _count_active_orders(state) -> int:
        return sum(1 for _, fs in state.figi.items() if getattr(fs, "active_order_id", None))

    @staticmethod
    def _count_pending_buys(state) -> int:
        """
        Conservative: treat any active order on a figi WITHOUT position as a pending BUY.
        (We don't always know side here. For this bot's entry logic it's enough.)
        """
        cnt = 0
        for _, fs in state.figi.items():
            has_pos = int(getattr(fs, "position_lots", 0) or 0) > 0
            has_order = bool(getattr(fs, "active_order_id", None))
            if has_order and not has_pos:
                cnt += 1
        return cnt

    def allow_new_trade(self, state, account_id: str, figi: str) -> bool:
        """
        Проверки именно на ОТКРЫТИЕ новой позиции (BUY).
        """
        # 1) дневной лок
        if self._locked:
            return False

        # 2) лимит сделок в день
        if int(getattr(state, "trades_today", 0) or 0) >= int(self.max_trades_per_day):
            return False

        # 3) уже есть позиция по этому figi
        if state.has_open_position(figi):
            return False

        fs = state.figi.get(figi)

        # 4) если у figi уже висит активная заявка — не ставим новую (если запрещено)
        if fs and getattr(fs, "active_order_id", None) and int(self.max_active_orders_per_figi) <= 1:
            return False

        # 5) портфельные ограничения: позиции + pending BUY считаем как "занятые слоты"
        open_positions = self._count_open_positions(state)
        pending_buys = self._count_pending_buys(state)
        if (open_positions + pending_buys) >= int(self.max_positions):
            return False

        # 6) общий лимит pending BUY (на случай, если хочешь max_positions=3,
        # но pending BUY ограничить 1-2)
        if pending_buys >= int(self.max_pending_buys_total):
            return False

        # 7) общий лимит активных ордеров любого типа (защита от спама)
        active_orders = self._count_active_orders(state)
        if active_orders >= int(self.max_active_orders_total):
            return False

        return True
