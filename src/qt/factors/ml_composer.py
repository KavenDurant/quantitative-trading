from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

from qt.common.logger import get_logger
from qt.factors.constants import ML_FALLBACK_FACTOR_WEIGHTS

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
    ) -> None:
        self.n_splits = max(n_splits, 1)
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
    ) -> tuple[pd.DataFrame, pd.Series]:
        """准备 LightGBM 训练数据。"""
        self._validate_features(factor_history)
        if "trade_date" not in factor_history.columns:
            raise ValueError("factor_history 缺少 trade_date 列")
        if "code" not in factor_history.columns:
            raise ValueError("factor_history 缺少 code 列")
        if not {"trade_date", "code", "forward_return"}.issubset(forward_returns.columns):
            raise ValueError("forward_returns 缺少必要列: trade_date / code / forward_return")

        merged = pd.merge(
            factor_history[["trade_date", "code", *FEATURE_COLS]],
            forward_returns[["trade_date", "code", "forward_return"]],
            on=["trade_date", "code"],
            how="inner",
        ).dropna()

        if merged.empty:
            raise ValueError("训练数据为空")
        if len(merged) < 50:
            logger.warning("训练样本量不足: %d", len(merged))

        merged = merged.sort_values("trade_date").reset_index(drop=True)
        X = merged[FEATURE_COLS]
        y = merged["forward_return"]
        logger.info("准备训练数据: samples=%d features=%d", len(X), len(FEATURE_COLS))
        return X, y

    def _build_model(self) -> "lgb.LGBMRegressor":
        try:
            import lightgbm as lgb
        except ImportError as exc:
            logger.error("LightGBM 未安装，请运行: pip install lightgbm")
            raise ImportError("LightGBM 未安装") from exc

        return lgb.LGBMRegressor(
            learning_rate=self.learning_rate,
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=42,
            verbose=-1,
        )

    def _resolve_n_splits(self, sample_count: int) -> int:
        if sample_count < 3:
            return 1
        return min(self.n_splits, sample_count - 1)

    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        """训练 LightGBM 模型。"""
        split_count = self._resolve_n_splits(len(X))
        cv_scores: list[float] = []

        if split_count > 1:
            tscv = TimeSeriesSplit(n_splits=split_count)
            for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X), start=1):
                fold_model = self._build_model()
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
                fold_model.fit(X_train, y_train)

                val_pred = fold_model.predict(X_val)
                val_ic = np.corrcoef(val_pred, y_val)[0, 1]
                score = 0.0 if np.isnan(val_ic) else float(val_ic)
                cv_scores.append(score)
                logger.info("Fold %d: IC=%.4f", fold_idx, score)
        else:
            logger.warning("样本量过少，跳过时间序列交叉验证 samples=%d", len(X))

        self.model = self._build_model()
        self.model.fit(X, y)
        self.feature_importance = {
            feature: float(importance)
            for feature, importance in zip(FEATURE_COLS, self.model.feature_importances_)
        }

        if cv_scores:
            mean_ic = float(np.mean(cv_scores))
            std_ic = float(np.std(cv_scores))
            logger.info("交叉验证完成: Mean IC=%.4f (±%.4f)", mean_ic, std_ic)
        logger.info("特征重要性: %s", self.feature_importance)

    def predict(self, frame: pd.DataFrame) -> pd.Series:
        """预测因子合成分数。"""
        if self.model is None:
            raise RuntimeError("模型未训练，请先调用 train()")

        self._validate_features(frame)
        predictions = self.model.predict(frame[FEATURE_COLS])
        return pd.Series(predictions, index=frame.index, name="ml_composite_score")

    def fit_predict(
        self,
        factor_history: pd.DataFrame,
        forward_returns: pd.DataFrame,
        current_frame: pd.DataFrame,
    ) -> MLModelResult:
        """训练并预测当前截面。"""
        X, y = self.prepare_training_data(factor_history, forward_returns)
        self.train(X, y)
        predictions = self.predict(current_frame)
        return MLModelResult(
            predictions=predictions,
            feature_importance=self.feature_importance,
            model_type="LightGBM",
            trained_at=datetime.now().isoformat(),
        )


def _build_fallback_scores(frame: pd.DataFrame) -> pd.Series:
    missing = [col for col in FEATURE_COLS if col not in frame.columns]
    if missing:
        raise ValueError(f"缺少特征列: {missing}")
    score = sum(frame[col] * weight for col, weight in ML_FALLBACK_FACTOR_WEIGHTS.items())
    return pd.Series(score, index=frame.index, name="ml_composite_score")


def build_ml_composite_scores(
    frame: pd.DataFrame,
    factor_history: pd.DataFrame | None = None,
    forward_returns: pd.DataFrame | None = None,
    use_cross_validation: bool = True,
) -> pd.DataFrame:
    """使用 ML 模型合成因子分数。"""
    result = frame.copy()

    if factor_history is None or forward_returns is None:
        logger.warning("缺少历史数据，回退到等权合成")
        result["ml_composite_score"] = _build_fallback_scores(result)
        return result.sort_values("ml_composite_score", ascending=False).reset_index(drop=True)

    try:
        composer = MLFactorComposer(n_splits=5 if use_cross_validation else 1)
        model_result = composer.fit_predict(factor_history, forward_returns, result)
        result["ml_composite_score"] = model_result.predictions
        logger.info(
            "ML 合成完成: importance=%s samples=%d",
            model_result.feature_importance,
            len(result),
        )
    except (ImportError, ValueError) as exc:
        logger.warning("ML 训练失败，回退到等权合成: %s", exc)
        result["ml_composite_score"] = _build_fallback_scores(result)

    return result.sort_values("ml_composite_score", ascending=False).reset_index(drop=True)


def select_stocks_ml(
    frame: pd.DataFrame,
    top_n: int = 5,
    factor_history: pd.DataFrame | None = None,
    forward_returns: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """使用 ML 合成选择股票。"""
    scored = build_ml_composite_scores(frame, factor_history, forward_returns)
    result = scored.head(top_n)
    output_cols = ["code"]
    if "name" in result.columns:
        output_cols.append("name")
    output_cols += ["quality_score", "value_score", "expectation_score", "ml_composite_score"]
    if "close" in result.columns:
        output_cols.append("close")
    return result[[column for column in output_cols if column in result.columns]]
