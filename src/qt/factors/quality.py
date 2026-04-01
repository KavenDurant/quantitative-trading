from __future__ import annotations

import pandas as pd

from qt.factors.normalize import percentile_rank


QUALITY_COLUMNS = ["roe", "gross_margin", "operating_cashflow_ratio"]


def compute_quality_score(frame: pd.DataFrame) -> pd.Series:
    parts = [percentile_rank(frame[column], ascending=True) for column in QUALITY_COLUMNS if column in frame.columns]
    if "debt_to_asset" in frame.columns:
        parts.append(percentile_rank(frame["debt_to_asset"], ascending=False))
    if not parts:
        return pd.Series(0.0, index=frame.index)
    return sum(parts) / len(parts)
