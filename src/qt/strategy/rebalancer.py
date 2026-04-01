from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class RebalanceSignal:
    code: str
    action: str
    target_weight: float
    target_shares: int
    price: float


def build_rebalance_signals(
    targets: pd.DataFrame,
    current_positions: dict[str, int],
    current_prices: dict[str, float] | None = None,
) -> list[RebalanceSignal]:
    signals: list[RebalanceSignal] = []
    target_codes = set(targets["code"])
    current_prices = current_prices or {}

    for code, shares in current_positions.items():
        if code not in target_codes and shares > 0:
            signals.append(
                RebalanceSignal(
                    code=code,
                    action="SELL",
                    target_weight=0.0,
                    target_shares=0,
                    price=float(current_prices.get(code, 0.0)),
                )
            )

    for row in targets.itertuples(index=False):
        current_shares = current_positions.get(row.code, 0)
        if row.target_shares != current_shares:
            action = "BUY" if row.target_shares > current_shares else "SELL"
            signals.append(
                RebalanceSignal(
                    code=row.code,
                    action=action,
                    target_weight=float(row.target_weight),
                    target_shares=int(row.target_shares),
                    price=float(row.last_price),
                )
            )
    return signals
