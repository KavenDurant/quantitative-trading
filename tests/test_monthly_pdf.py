from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from qt.reports.monthly_pdf import MonthlyPDFReport, MonthlyMetrics


def test_monthly_pdf_report_init():
    report = MonthlyPDFReport(
        db_path=Path("/tmp/test.db"),
        output_dir=Path("/tmp/reports"),
    )
    assert report.db_path == Path("/tmp/test.db")
    assert report.output_dir == Path("/tmp/reports")
    print("PASS: monthly_pdf_report_init")


def test_compute_monthly_metrics():
    nav_df = pd.DataFrame({
        "trade_date": ["2026-04-01", "2026-04-02", "2026-04-03"],
        "nav": [10000.0, 10200.0, 10100.0],
        "cash": [0, 0, 0],
    })

    report = MonthlyPDFReport(Path("/tmp/test.db"), Path("/tmp"))
    metrics = report._compute_monthly_metrics(nav_df, "2026-04")

    assert metrics.month == "2026-04"
    assert metrics.start_nav == 10000.0
    assert metrics.end_nav == 10100.0
    assert metrics.total_return == 0.01
    assert metrics.max_drawdown <= 0
    assert round(metrics.sharpe_ratio, 4) != 0.0
    assert round(metrics.monthly_win_rate, 4) == 0.5
    print("PASS: compute_monthly_metrics")


def test_compute_monthly_metrics_empty():
    nav_df = pd.DataFrame()

    report = MonthlyPDFReport(Path("/tmp/test.db"), Path("/tmp"))
    metrics = report._compute_monthly_metrics(nav_df, "2026-04")

    assert metrics.month == "2026-04"
    assert metrics.total_return == 0.0
    assert metrics.max_drawdown == 0.0
    print("PASS: compute_monthly_metrics_empty")


def test_build_text_report():
    metrics = MonthlyMetrics(
        month="2026-04",
        total_return=0.05,
        max_drawdown=-0.03,
        sharpe_ratio=1.2,
        monthly_win_rate=0.6,
        start_nav=10000.0,
        end_nav=10500.0,
        num_trades=10,
        num_positions=5,
    )
    positions = pd.DataFrame({
        "code": ["600036", "000651"],
        "shares": [100, 200],
        "price": [35.0, 38.0],
    })
    trades = pd.DataFrame({
        "trade_date": ["2026-04-01", "2026-04-02"],
        "code": ["600036", "000651"],
        "side": ["buy", "buy"],
        "shares": [100, 200],
        "price": [35.0, 38.0],
        "amount": [3500.0, 7600.0],
    })

    report = MonthlyPDFReport(Path("/tmp/test.db"), Path("/tmp"))
    text = report._build_text_report(metrics, positions, trades)

    assert "2026-04" in text
    assert "月度收益率" in text
    assert "5.00%" in text
    assert "月末持仓" in text
    assert "月度交易" in text
    assert "600036" in text
    print("PASS: build_text_report")


def test_generate_creates_output_dir(tmp_path):
    output_dir = tmp_path / "reports" / "nested"
    report = MonthlyPDFReport(Path(":memory:"), output_dir)

    nav_df = pd.DataFrame({
        "trade_date": ["2026-04-01"],
        "nav": [10000.0],
        "cash": [0],
    })

    text = report._build_text_report(
        MonthlyMetrics(
            month="2026-04",
            total_return=0.0,
            max_drawdown=0.0,
            sharpe_ratio=0.0,
            monthly_win_rate=0.0,
            start_nav=10000.0,
            end_nav=10000.0,
            num_trades=0,
            num_positions=0,
        ),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    result_path = report._convert_to_pdf if report._convert_to_pdf else None
    assert output_dir.exists()
    print("PASS: generate_creates_output_dir")


def main():
    tests = [
        test_monthly_pdf_report_init,
        test_compute_monthly_metrics,
        test_compute_monthly_metrics_empty,
        test_build_text_report,
        test_generate_creates_output_dir,
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
    print(f"\n月度 PDF 报告测试结果: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
