from __future__ import annotations

from qt.execution.broker_base import BrokerBase
from qt.strategy.rebalancer import RebalanceSignal


class OrderManager:
    def __init__(self, broker: BrokerBase) -> None:
        self.broker = broker

    def execute(self, rebalance_id: str, signals: list[RebalanceSignal]) -> list[tuple[str, str, int, float, float]]:
        targets_by_code = {code: shares for code, shares in self.broker.positions().items()}
        prices_by_code: dict[str, float] = {}

        for signal in signals:
            targets_by_code[signal.code] = signal.target_shares
            prices_by_code[signal.code] = signal.price

        payload = [
            (code, shares, prices_by_code.get(code, 0.0))
            for code, shares in targets_by_code.items()
        ]
        self.broker.apply_orders(rebalance_id, payload)
        return getattr(self.broker, "last_trades", [])
