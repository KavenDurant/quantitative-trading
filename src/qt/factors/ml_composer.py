from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from typing import TYPE_CHECKING

from qt.common.logger import get_logger

if TYPE_CHECKING:
    import lightgbm as lgb

logger = get_logger(__name__)


FEATURE_COLS = ["quality_score", "value_score", "expectation_score"]


@dataclass(slots=True)
class MLModelResult:
    predictions: pd.Series
    feature_importance: dict[str, float]
    model_type: str
    trained_at: str


class MLFactorComposer:
    def __init__(
        self,
        n_splits: int = 5,
        learning_rate: float = 0.05,
        n_estimators: int = 100,
        max_depth: int = 4,
    ):
        self.n_splits = n_splits
        self.learning_rate = learning_rate
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.model: lgb.LGBMRegressor | None = None
        self.feature_importance: dict[str, float] = {}

    def _validate_features(self, frame: pd.DataFrame) -> None:
        missing = [col for col in FEATURE_COLS if col not in frame.columns]
        if missing:
            raise ValueError(f"缺少特征列: {missing}")

    def prepare_training_data(
        self,
        factor_history: pd.DataFrame,
        forward_returns: pd.DataFrame,
        forward_days: int = 20,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """
        准备训练数据
        - X: 因子值
        - y: 未来 N 日收益率
        """
        merged = pd.merge(
            factor_history[["trade_date"] + FEATURE_COLS],
            forward_returns[["trade_date", "forward_return"]],
            on="trade_date",
            how="inner",
        ).dropna()

        if len(merged) < 50:
            logger.warning("训练样本量不足: %d", len(merged))

        X = merged[FEATURE_COLS]
        y = merged["forward_return"]

        logger.info("准备训练数据: samples=%d features=%d", len(X), len(FEATURE_COLS))
        return X, y

    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        """训练 LightGBM 模型"""
        try:
            import lightgbm as lgb
        except ImportError:
            logger.error("LightGBM 未安装，请运行: pip install lightgbm")
            raise

        self.model = lgb.LGBMRegressor(
            learning_rate=self.learning_rate,
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=42,
            verbose=-1,
        )

        tscv = TimeSeriesSplit(n_splits=self.n_splits)
        cv_scores = []

        for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X)):
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

            self.model.fit(X_train, y_train, eval_set=[(X_val, y_val)], callbacks=[])

            val_pred = self.model.predict(X_val)
            val_ic = np.corrcoef(val_pred, y_val)[0, 1]
            cv_scores.append(val_ic if not np.isnan(val_ic) else 0.0)

            logger.info("Fold %d: IC=%.4f", fold_idx + 1, val_ic)

        mean_ic = np.mean(cv_scores)
        std_ic = np.std(cv_scores)
        logger.info("交叉验证完成: Mean IC=%.4f (±%.4f)", mean_ic, std_ic)

        self.feature_importance = dict(zip(
            FEATURE_COLS,
            self.model.feature_importances_,
        ))
        logger.info("特征重要性: %s", self.feature_importance)

    def predict(self, frame: pd.DataFrame) -> pd.Series:
        """预测因子合成分数"""
        if self.model is None:
            raise RuntimeError("模型未训练，请先调用 train()")

        self._validate_features(frame)

        X = frame[FEATURE_COLS].copy()
        predictions = self.model.predict(X)

        return pd.Series(predictions, index=frame.index, name="ml_composite_score")

    def fit_predict(
        self,
        factor_history: pd.DataFrame,
        forward_returns: pd.DataFrame,
        current_frame: pd.DataFrame,
    ) -> MLModelResult:
        """训练并预测当前截面"""
        X, y = self.prepare_training_data(factor_history, forward_returns)
        self.train(X, y)

        predictions = self.predict(current_frame)

        return MLModelResult(
            predictions=predictions,
            feature_importance=self.feature_importance,
            model_type="LightGBM",
            trained_at=datetime.now().isoformat(),
        )


def build_ml_composite_scores(
    frame: pd.DataFrame,
    factor_history: pd.DataFrame | None = None,
    forward_returns: pd.DataFrame | None = None,
    use_cross_validation: bool = True,
) -> pd.DataFrame:
    """
    使用 ML 模型合成因子分数

    Args:
        frame: 当前截面的因子数据
        factor_history: 历史因子数据（用于训练）
        forward_returns: 历史未来收益数据（用于训练）
        use_cross_validation: 是否使用交叉验证训练

    Returns:
        包含 ml_composite_score 列的 DataFrame
    """
    result = frame.copy()

    if factor_history is None or forward_returns is None:
        logger.warning("缺少历史数据，回退到等权合成")
        result["ml_composite_score"] = (
            result.get("quality_score", 0) * 0.4
            + result.get("value_score", 0) * 0.35
            + result.get("expectation_score", 0) * 0.25
        )
        return result

    try:
        composer = MLFactorComposer(
            n_splits=5 if use_cross_validation else 1,
            learning_rate=0.05,
            n_estimators=100,
            max_depth=4,
        )

        model_result = composer.fit_predict(factor_history, forward_returns, frame)

        result["ml_composite_score"] = model_result.predictions

        logger.info(
            "ML 合成完成: importance=%s samples=%d",
            model_result.feature_importance,
            len(result),
        )

    except Exception as exc:
        logger.warning("ML 训练失败，回退到等权合成: %s", exc)
        result["ml_composite_score"] = (
            result.get("quality_score", 0) * 0.4
            + result.get("value_score", 0) * 0.35
            + result.get("expectation_score", 0) * 0.25
        )

    return result.sort_values("ml_composite_score", ascending=False).reset_index(drop=True)


def select_stocks_ml(
    frame: pd.DataFrame,
    top_n: int = 5,
    factor_history: pd.DataFrame | None = None,
    forward_returns: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """使用 ML 合成选择股票"""
    scored = build_ml_composite_scores(frame, factor_history, forward_returns)
    result = scored.head(top_n)
    output_cols = ["code"]
    if "name" in result.columns:
        output_cols.append("name")
    output_cols += ["quality_score", "value_score", "expectation_score", "ml_composite_score"]
    if "close" in result.columns:
        output_cols.append("close")
    return result[[c for c in output_cols if c in result.columns]]
