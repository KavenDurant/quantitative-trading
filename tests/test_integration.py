from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from qt.backtest.engine import compute_nav
from qt.backtest.metrics import compute_annualized_return, compute_max_drawdown, compute_sharpe_ratio
from qt.common.config import load_app_config
from qt.data.ingest.universe_builder import filter_by_liquidity, filter_by_price, filter_universe, RawInstrument
from qt.data.quality_check import DataQualityChecker
from qt.data.storage.repository import (
    DailyPrice, FundamentalSnapshot, Instrument, Repository,
    ValuationSnapshot, AnalystExpectation, EarningsSurprise,
)
from qt.data.storage.sqlite_client import SQLiteClient
from qt.execution.paper_broker import PaperBroker
from qt.factors.combiner import build_composite_scores, select_stocks
from qt.factors.normalize import winsorize, check_no_future_leak
from qt.monitoring.notifier import Notifier
from qt.strategy.position_sizer import build_position_table
from qt.strategy.risk_controls import (
    check_holding_period, check_portfolio_stop_loss,
    check_single_position_weight, check_stop_loss, check_take_profit,
)
from qt.strategy.risk_manager import RiskManager


def test_config():
    config = load_app_config(Path(__file__).resolve().parents[1])
    assert config.initial_cash > 0
    assert config.stop_loss_pct < 0
    assert config.take_profit_1_pct > 0
    print("PASS: config")


def test_database():
    db_path = Path(__file__).resolve().parents[1] / ".data" / "test_integration.db"
    db_path.parent.mkdir(exist_ok=True)
    client = SQLiteClient(db_path)
    client.init_db()
    conn = client.connect()
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "instrument_master" in tables
    assert "valuation" in tables
    assert "analyst_expectation" in tables
    assert "earnings_surprise" in tables
    assert len(tables) >= 15
    conn.close()
    db_path.unlink(missing_ok=True)
    print("PASS: database")


def test_position_sizer():
    df = pd.DataFrame({
        "code": ["600036", "000651", "601318"],
        "last_price": [35.0, 38.0, 45.0],
        "rank_value": [1, 2, 3],
    })
    table = build_position_table(df, 100000, 100, 0.02)
    assert not table.empty
    assert "buy_limit" in table.columns
    assert "stop_loss" in table.columns
    assert "take_profit_1" in table.columns
    print("PASS: position_sizer")


def test_price_calculation():
    df = pd.DataFrame({
        "code": ["600036"], "last_price": [35.0], "rank_value": [1],
    })
    table = build_position_table(df, 100000, 100, 0.02, 0.005, -0.08, 0.15, 0.25)
    row = table.iloc[0]
    assert row["buy_limit"] > row["last_price"]
    assert row["stop_loss"] < row["buy_limit"]
    assert row["take_profit_1"] > row["buy_limit"]
    assert row["take_profit_2"] > row["take_profit_1"]
    print("PASS: price_calculation")


def test_risk_controls():
    assert check_stop_loss(32.0, 35.0, -0.08) is True
    assert check_stop_loss(34.0, 35.0, -0.08) is False
    assert check_take_profit(42.0, 35.0, 0.15) is True
    assert check_holding_period("2025-01-01", "2025-05-15", 120) is True
    assert check_portfolio_stop_loss(8800, 10000, -0.12) is True
    assert check_single_position_weight(2500, 10000, 0.2) is True
    print("PASS: risk_controls")


def test_risk_manager():
    rm = RiskManager(stop_loss_pct=-0.08, take_profit_1_pct=0.15, take_profit_2_pct=0.25)
    alerts = rm.check_positions(
        positions={"600036": 100},
        avg_costs={"600036": 35.0},
        current_prices={"600036": 31.0},
        buy_dates={"600036": "2025-01-01"},
        current_date="2025-06-01",
        current_nav=3100,
        month_start_nav=3500,
    )
    types = {a.alert_type for a in alerts}
    assert "个股止损" in types
    print("PASS: risk_manager")


def test_notifier():
    n = Notifier()
    result = n.send("test", "integration test")
    assert result is False  # no sendkey configured
    print("PASS: notifier")


def test_factors():
    df = pd.DataFrame({
        "code": ["A", "B", "C"],
        "roe": [0.3, 0.15, 0.2],
        "gross_margin": [0.9, 0.3, 0.5],
        "operating_cashflow_ratio": [1.2, 0.8, 1.0],
        "pe_ttm": [30, 8, 15],
        "pb": [10, 1.5, 3],
        "ps_ttm": [15, 2, 5],
        "net_profit_yoy": [0.15, 0.2, 0.1],
        "revenue_yoy": [0.12, 0.15, 0.08],
    })
    result = select_stocks(df, top_n=2)
    assert len(result) == 2
    assert "composite_score" in result.columns
    print("PASS: factors")


def main():
    tests = [
        test_config, test_database, test_position_sizer,
        test_price_calculation, test_risk_controls, test_risk_manager,
        test_notifier, test_factors,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n集成测试结果: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
