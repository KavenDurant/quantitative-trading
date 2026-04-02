import pandas as pd

from qt.strategy.risk_controls import cap_position_count, check_market_trend
from qt.strategy.risk_manager import RiskManager


def test_cap_position_count_respects_max():
    assert cap_position_count(10, 5) == 5
    assert cap_position_count(3, 5) == 3


def test_check_market_trend_requires_enough_points():
    assert check_market_trend([1.0] * 59, short_window=20, long_window=60) is False


def test_check_market_trend_uptrend_passes():
    prices = [float(i) for i in range(1, 61)]
    assert check_market_trend(prices, short_window=20, long_window=60) is True






def test_check_market_trend_downtrend_blocks():
    prices = [float(i) for i in range(60, 0, -1)]
    assert check_market_trend(prices, short_window=20, long_window=60) is False


def test_risk_manager_market_timing_blocks_candidates_when_trend_down():
    candidates = pd.DataFrame(
        {
            "code": ["600036", "000651"],
            "last_price": [35.0, 40.0],
            "amount": [50_000_000, 60_000_000],
            "list_date": ["2020-01-01", "2019-01-01"],
            "is_st": [0, 0],
            "benchmark_prices": [[120.0] * 55 + [90.0] * 10, [120.0] * 55 + [90.0] * 10],
        }
    )
    manager = RiskManager(
        market_timing_enabled=True,
        market_timing_short_window=20,
        market_timing_long_window=60,
    )

    filtered = manager.pre_trade_filter(candidates, as_of_date="2026-04-01")
    assert filtered.empty


def test_risk_manager_market_timing_allows_candidates_when_trend_up():
    uptrend_series = [100.0 + i * 0.5 for i in range(80)]
    candidates = pd.DataFrame(
        {
            "code": ["600036", "000651"],
            "last_price": [35.0, 40.0],
            "amount": [50_000_000, 60_000_000],
            "list_date": ["2020-01-01", "2019-01-01"],
            "is_st": [0, 0],
            "benchmark_prices": [uptrend_series, uptrend_series],
        }
    )
    manager = RiskManager(
        market_timing_enabled=True,
        market_timing_short_window=20,
        market_timing_long_window=60,
    )

    filtered = manager.pre_trade_filter(candidates, as_of_date="2026-04-01")
    assert len(filtered) == 2
