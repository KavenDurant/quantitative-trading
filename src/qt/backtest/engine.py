from __future__ import annotations

import pandas as pd


def compute_nav(cash: float, positions: dict[str, int], prices: dict[str, float]) -> float:
    holdings_value = sum(shares * prices.get(code, 0.0) for code, shares in positions.items())
    return cash + holdings_value
