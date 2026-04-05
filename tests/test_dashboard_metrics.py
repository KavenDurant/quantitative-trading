from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from qt.backtest.metrics import compute_monthly_returns
from qt.monitoring.dashboard_app import render_nav_chart


def test_compute_monthly_returns_preserves_non_date_index_behavior():
    nav_series = pd.Series([100.0, 110.0, 99.0])

    result = compute_monthly_returns(nav_series)

    assert len(result) == 2
    assert round(float(result.iloc[0]), 4) == 0.1
    assert round(float(result.iloc[1]), 4) == -0.1


def test_compute_monthly_returns_calendar_month_uses_month_end_values():
    nav_series = pd.Series(
        [100.0, 101.0, 105.0, 106.0, 110.0],
        index=pd.to_datetime([
            "2026-01-05",
            "2026-01-31",
            "2026-02-10",
            "2026-02-28",
            "2026-03-31",
        ]),
    )

    result = compute_monthly_returns(nav_series, calendar_month=True)

    assert len(result) == 2
    assert round(float(result.iloc[0]), 4) == round((106.0 - 101.0) / 101.0, 4)
    assert round(float(result.iloc[1]), 4) == round((110.0 - 106.0) / 106.0, 4)


def test_render_nav_chart_uses_initial_cash_baseline(monkeypatch):
    nav = pd.DataFrame(
        {
            "trade_date": ["2026-01-31", "2026-02-28"],
            "nav": [9800.0, 10200.0],
        }
    )
    captured = {}

    def fake_plotly_chart(fig, use_container_width=True):
        captured["fig"] = fig

    monkeypatch.setattr("qt.monitoring.dashboard_app.st.plotly_chart", fake_plotly_chart)

    render_nav_chart(nav, 10000.0)

    shapes = list(captured["fig"].layout.shapes)
    assert shapes
    assert float(shapes[0].y0) == 10000.0
    assert float(shapes[0].y1) == 10000.0
