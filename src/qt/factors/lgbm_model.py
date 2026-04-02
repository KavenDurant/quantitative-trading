from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qt.common.logger import get_logger
from qt.factors.ml_composer import MLFactorComposer, build_ml_composite_scores

logger = get_logger(__name__)


@dataclass(slots=True)
class LGBMTrainingArtifacts:
    feature_importance: dict[str, float]
    model_type: str
    trained_at: str
    sample_count: int
    feature_count: int


def train_lightgbm_model(
    factor_history: pd.DataFrame,
    forward_returns: pd.DataFrame,
    n_splits: int = 5,
    learning_rate: float = 0.05,
    n_estimators: int = 100,
    max_depth: int = 4,
) -> tuple[MLFactorComposer, LGBMTrainingArtifacts]:
    """训练 LightGBM 因子合成模型并返回训练产物。"""
    composer = MLFactorComposer(
        n_splits=n_splits,
        learning_rate=learning_rate,
        n_estimators=n_estimators,
        max_depth=max_depth,
    )
    X, y = composer.prepare_training_data(factor_history, forward_returns)
    composer.train(X, y)
    artifacts = LGBMTrainingArtifacts(
        feature_importance=composer.feature_importance,
        model_type="LightGBM",
        trained_at=pd.Timestamp.now().isoformat(),
        sample_count=len(X),
        feature_count=X.shape[1],
    )
    logger.info("LightGBM 模型训练完成 samples=%d features=%d", artifacts.sample_count, artifacts.feature_count)
    return composer, artifacts


def predict_lightgbm_scores(model: MLFactorComposer, frame: pd.DataFrame) -> pd.Series:
    """使用已训练模型预测当前截面的 ML 分数。"""
    return model.predict(frame)


def build_lightgbm_scores(
    frame: pd.DataFrame,
    factor_history: pd.DataFrame | None = None,
    forward_returns: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """兼容旧模块名的 LightGBM 合成入口。"""
    return build_ml_composite_scores(frame, factor_history, forward_returns)
