from __future__ import annotations

import pandas as pd


def percentile_rank(series: pd.Series, ascending: bool = True) -> pd.Series:
    ranked = series.rank(pct=True, ascending=ascending, method="average")
    return ranked.fillna(0.0)


def winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lower=lo, upper=hi)


def check_no_future_leak(data_date: str, as_of_date: str) -> bool:
    return data_date <= as_of_date
