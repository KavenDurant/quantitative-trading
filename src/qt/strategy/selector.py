from __future__ import annotations

import pandas as pd

from qt.strategy.risk_controls import cap_position_count


def select_top_candidates(scored: pd.DataFrame, max_positions: int) -> pd.DataFrame:
    target_count = cap_position_count(len(scored), max_positions)
    return scored.nsmallest(target_count, columns=["rank_value"]).copy()
