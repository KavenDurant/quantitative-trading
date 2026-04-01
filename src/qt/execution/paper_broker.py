from __future__ import annotations

from dataclasses import dataclass, field

from qt.execution.broker_base import BrokerBase


@dataclass(slots=True)
class PaperBroker(BrokerBase):
    initial_cash: float
    commission_rate: float = 0.0
    slippage_rate: float = 0.0
    positions_map: dict[str, int] = field(default_factory=dict)
    avg_costs: dict[str, float] = field(default_factory=dict)
    available_cash: float = 0.0
    executed_rebalances: set[str] = field(default_factory=set)
    last_trades: list[tuple[str, str, int, float, float]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.available_cash = self.initial_cash

    def positions(self) -> dict[str, int]:
        return dict(self.positions_map)

    def cash(self) -> float:
        return self.available_cash

    def apply_orders(self, rebalance_id: str, targets: list[tuple[str, int, float]]) -> None:
        if rebalance_id in self.executed_rebalances:
            return

        self.last_trades = []
        for code, target_shares, price in targets:
            current_shares = self.positions_map.get(code, 0)
            delta = target_shares - current_shares
            if delta == 0:
                continue
            traded_price = price * (1 + self.slippage_rate if delta > 0 else 1 - self.slippage_rate)
            gross_amount = delta * traded_price
            commission = abs(gross_amount) * self.commission_rate
            net_amount = gross_amount + commission if delta > 0 else gross_amount - commission
            self.available_cash -= net_amount
            side = "BUY" if delta > 0 else "SELL"
            self.last_trades.append((code, side, abs(delta), traded_price, abs(gross_amount)))
            if target_shares == 0:
                self.positions_map.pop(code, None)
                self.avg_costs.pop(code, None)
            else:
                self.positions_map[code] = target_shares
                self.avg_costs[code] = traded_price
        self.executed_rebalances.add(rebalance_id)
