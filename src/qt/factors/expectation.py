from __future__ import annotations

import pandas as pd

from qt.factors.normalize import percentile_rank


EXPECTATION_COLUMNS = ["net_profit_yoy", "revenue_yoy"]


def compute_expectation_score(frame: pd.DataFrame) -> pd.Series:
    parts = [percentile_rank(frame[column], ascending=True) for column in EXPECTATION_COLUMNS if column in frame.columns]
    if "eps_revision_pct" in frame.columns:
        parts.append(percentile_rank(frame["eps_revision_pct"], ascending=True))
    if "surprise_pct" in frame.columns:
        parts.append(percentile_rank(frame["surprise_pct"], ascending=True))
    if "coverage_count" in frame.columns:
        parts.append(percentile_rank(frame["coverage_count"], ascending=True))
    if "rating_score" in frame.columns:
        parts.append(percentile_rank(frame["rating_score"], ascending=True))
    if not parts:
        return pd.Series(0.0, index=frame.index)
    return sum(parts) / len(parts)


def compute_sue(actual: pd.Series, expected: pd.Series, std: pd.Series | None = None) -> pd.Series:
    diff = actual - expected
    if std is not None and (std > 0).any():
        return (diff / std.replace(0, float("nan"))).fillna(0.0)
    return diff.fillna(0.0)


def compute_earnings_surprise_score(frame: pd.DataFrame) -> pd.Series:
    if "surprise_pct" in frame.columns:
        return percentile_rank(frame["surprise_pct"], ascending=True)
    return pd.Series(0.0, index=frame.index)
