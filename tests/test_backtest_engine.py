import pandas as pd

from qt.backtest.metrics import compute_max_drawdown
from qt.common.calendar import month_end_dates


def test_month_end_dates_generates_monthly_series():
    dates = month_end_dates("2025-01-01", "2025-03-31")
    assert dates == ["2025-01-31", "2025-02-28", "2025-03-31"]


def test_compute_max_drawdown_returns_negative_value():
    nav = pd.Series([1.0, 1.1, 0.9, 1.2])
    assert compute_max_drawdown(nav) < 0
