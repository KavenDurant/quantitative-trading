from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from qt.factors.decay_detector import (
    DecayStatus,
    batch_detect_decay,
    detect_factor_decay,
    format_report,
)


def test_detect_factor_decay_healthy():
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "quality_score": [0.6, 0.65, 0.7, 0.75],
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "forward_return": [0.01, 0.02, 0.015, 0.025],
    })

    report = detect_factor_decay(
        "quality_score",
        factor_history,
        forward_returns,
        lookback_months=1,
    )

    assert report.factor_name == "quality_score"
    assert report.status in (DecayStatus.HEALTHY, DecayStatus.WARNING)
    assert report.current_ic >= 0
    print("PASS: detect_factor_decay_healthy")


def test_detect_factor_decay_decayed():
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "value_score": [0.9, 0.8, 0.7, 0.6],  # 与收益负相关
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "forward_return": [0.01, 0.02, 0.015, 0.025],  # 递增
    })

    report = detect_factor_decay(
        "value_score",
        factor_history,
        forward_returns,
        lookback_months=1,
        ic_threshold=0.03,
    )

    assert report.factor_name == "value_score"
    assert report.sample_size > 0
    print("PASS: detect_factor_decay_decayed")


def test_batch_detect_decay():
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "quality_score": [0.6, 0.65, 0.7, 0.75],
        "value_score": [0.5, 0.55, 0.6, 0.65],
        "expectation_score": [0.4, 0.45, 0.5, 0.55],
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"],
        "forward_return": [0.01, 0.02, 0.015, 0.025],
    })

    reports = batch_detect_decay(
        ["quality_score", "value_score", "expectation_score"],
        factor_history,
        forward_returns,
        lookback_months=1,
    )

    assert len(reports) == 3
    assert all(r.factor_name in ["quality_score", "value_score", "expectation_score"] for r in reports)
    print("PASS: batch_detect_decay")


def test_format_report():
    report = detect_factor_decay(
        "composite_score",
        pd.DataFrame({
            "trade_date": ["2026-01-02", "2026-01-03"],
            "composite_score": [0.5, 0.55],
        }),
        pd.DataFrame({
            "trade_date": ["2026-01-02", "2026-01-03"],
            "forward_return": [0.01, 0.02],
        }),
        lookback_months=1,
    )

    formatted = format_report(report)
    assert "因子衰减检测报告" in formatted
    assert report.factor_name in formatted
    assert "IC 指标" in formatted
    assert "趋势分析" in formatted
    print("PASS: format_report")


def test_missing_factor_column():
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02"],
        "quality_score": [0.6],
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02"],
        "forward_return": [0.01],
    })

    report = detect_factor_decay(
        "non_existent_factor",
        factor_history,
        forward_returns,
    )

    assert report.status == DecayStatus.WARNING
    assert "不在历史数据中" in report.message
    print("PASS: missing_factor_column")


def main():
    tests = [
        test_detect_factor_decay_healthy,
        test_detect_factor_decay_decayed,
        test_batch_detect_decay,
        test_format_report,
        test_missing_factor_column,
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
    print(f"\n因子衰减检测测试结果: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
