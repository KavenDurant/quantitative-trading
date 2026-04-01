from __future__ import annotations

import pandas as pd

from qt.factors.normalize import percentile_rank


VALUE_COLUMNS = ["pe_ttm", "pb", "ps_ttm"]


def compute_value_score(frame: pd.DataFrame) -> pd.Series:
    parts = [percentile_rank(frame[column], ascending=False) for column in VALUE_COLUMNS if column in frame.columns]
    if "dividend_yield" in frame.columns:
        parts.append(percentile_rank(frame["dividend_yield"], ascending=True))
    if not parts:
        return pd.Series(0.0, index=frame.index)
    return sum(parts) / len(parts)


def compute_industry_relative_pe(frame: pd.DataFrame) -> pd.Series:
    if "industry" in frame.columns and "pe_ttm" in frame.columns:
        return frame.groupby("industry")["pe_ttm"].rank(pct=True, ascending=False, method="average").fillna(0.5)
    return percentile_rank(frame.get("pe_ttm", pd.Series(dtype=float)), ascending=False)


def compute_industry_relative_pb(frame: pd.DataFrame) -> pd.Series:
    if "industry" in frame.columns and "pb" in frame.columns:
        return frame.groupby("industry")["pb"].rank(pct=True, ascending=False, method="average").fillna(0.5)
    return percentile_rank(frame.get("pb", pd.Series(dtype=float)), ascending=False)
