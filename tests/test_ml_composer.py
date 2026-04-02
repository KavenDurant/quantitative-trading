from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
import numpy as np

from qt.factors.ml_composer import (
    MLFactorComposer,
    MLModelResult,
    build_ml_composite_scores,
    select_stocks_ml,
)


def test_ml_factor_composer_init():
    composer = MLFactorComposer(
        n_splits=3,
        learning_rate=0.1,
        n_estimators=50,
        max_depth=3,
    )
    assert composer.n_splits == 3
    assert composer.learning_rate == 0.1
    assert composer.n_estimators == 50
    assert composer.max_depth == 3
    assert composer.model is None
    print("PASS: ml_factor_composer_init")


def test_prepare_training_data():
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"] * 10,
        "quality_score": [0.6, 0.65, 0.7, 0.75] * 10,
        "value_score": [0.5, 0.55, 0.6, 0.65] * 10,
        "expectation_score": [0.4, 0.45, 0.5, 0.55] * 10,
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-07"] * 10,
        "forward_return": np.random.randn(40) * 0.02,
    })

    composer = MLFactorComposer()
    X, y = composer.prepare_training_data(factor_history, forward_returns)

    assert X.shape[1] == 3
    assert len(X) == len(y)
    assert len(X) > 0
    print("PASS: prepare_training_data")


def test_train_and_predict():
    try:
        import lightgbm as lgb
    except ImportError:
        print("SKIP: train_and_predict (lightgbm not installed)")
        return

    np.random.seed(42)
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02"] * 50 + ["2026-01-03"] * 50,
        "quality_score": np.random.rand(100),
        "value_score": np.random.rand(100),
        "expectation_score": np.random.rand(100),
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02"] * 50 + ["2026-01-03"] * 50,
        "forward_return": np.random.randn(100) * 0.02,
    })

    composer = MLFactorComposer(n_splits=2, n_estimators=20)
    X, y = composer.prepare_training_data(factor_history, forward_returns)
    composer.train(X, y)

    assert composer.model is not None
    assert len(composer.feature_importance) == 3

    current_frame = pd.DataFrame({
        "code": [f"60{i:04d}" for i in range(10)],
        "quality_score": np.random.rand(10),
        "value_score": np.random.rand(10),
        "expectation_score": np.random.rand(10),
    })
    predictions = composer.predict(current_frame)

    assert len(predictions) == 10
    assert predictions.name == "ml_composite_score"
    print("PASS: train_and_predict")


def test_fit_predict():
    try:
        import lightgbm as lgb
    except ImportError:
        print("SKIP: fit_predict (lightgbm not installed)")
        return

    np.random.seed(42)
    factor_history = pd.DataFrame({
        "trade_date": ["2026-01-02"] * 60 + ["2026-01-03"] * 60,
        "quality_score": np.random.rand(120),
        "value_score": np.random.rand(120),
        "expectation_score": np.random.rand(120),
    })
    forward_returns = pd.DataFrame({
        "trade_date": ["2026-01-02"] * 60 + ["2026-01-03"] * 60,
        "forward_return": np.random.randn(120) * 0.02,
    })
    current_frame = pd.DataFrame({
        "code": [f"60{i:04d}" for i in range(20)],
        "quality_score": np.random.rand(20),
        "value_score": np.random.rand(20),
        "expectation_score": np.random.rand(20),
    })

    composer = MLFactorComposer(n_estimators=20)
    result = composer.fit_predict(factor_history, forward_returns, current_frame)

    assert isinstance(result, MLModelResult)
    assert len(result.predictions) == 20
    assert result.model_type == "LightGBM"
    assert len(result.feature_importance) == 3
    print("PASS: fit_predict")


def test_build_ml_composite_scores():
    current_frame = pd.DataFrame({
        "code": [f"60{i:04d}" for i in range(10)],
        "quality_score": [0.6, 0.65, 0.7, 0.75, 0.8, 0.5, 0.55, 0.6, 0.65, 0.7],
        "value_score": [0.5, 0.55, 0.6, 0.65, 0.7, 0.45, 0.5, 0.55, 0.6, 0.65],
        "expectation_score": [0.4, 0.45, 0.5, 0.55, 0.6, 0.35, 0.4, 0.45, 0.5, 0.55],
    })

    result = build_ml_composite_scores(current_frame, factor_history=None, forward_returns=None)

    assert "ml_composite_score" in result.columns
    assert len(result) == 10
    print("PASS: build_ml_composite_scores (fallback)")


def test_select_stocks_ml():
    current_frame = pd.DataFrame({
        "code": [f"60{i:04d}" for i in range(10)],
        "name": [f"股票{i}" for i in range(10)],
        "quality_score": [0.6, 0.65, 0.7, 0.75, 0.8, 0.5, 0.55, 0.6, 0.65, 0.7],
        "value_score": [0.5, 0.55, 0.6, 0.65, 0.7, 0.45, 0.5, 0.55, 0.6, 0.65],
        "expectation_score": [0.4, 0.45, 0.5, 0.55, 0.6, 0.35, 0.4, 0.45, 0.5, 0.55],
        "close": [10.0 + i for i in range(10)],
    })

    result = select_stocks_ml(current_frame, top_n=5, factor_history=None, forward_returns=None)

    assert len(result) == 5
    assert "code" in result.columns
    assert "ml_composite_score" in result.columns
    print("PASS: select_stocks_ml")


def main():
    tests = [
        test_ml_factor_composer_init,
        test_prepare_training_data,
        test_train_and_predict,
        test_fit_predict,
        test_build_ml_composite_scores,
        test_select_stocks_ml,
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
    print(f"\nML 因子合成测试结果: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
