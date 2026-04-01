from __future__ import annotations

from pathlib import Path

import pandas as pd

from qt.factors.expectation import compute_expectation_score
from qt.factors.quality import compute_quality_score
from qt.factors.value import compute_value_score


DEFAULT_WEIGHTS = {
    "quality": 0.4,
    "value": 0.35,
    "expectation": 0.25,
}


def build_composite_scores(frame: pd.DataFrame, weights: dict[str, float] | None = None) -> pd.DataFrame:
    effective_weights = weights or DEFAULT_WEIGHTS
    scored = frame.copy()
    scored["quality_score"] = compute_quality_score(scored)
    scored["value_score"] = compute_value_score(scored)
    scored["expectation_score"] = compute_expectation_score(scored)
    scored["composite_score"] = (
        scored["quality_score"] * effective_weights["quality"]
        + scored["value_score"] * effective_weights["value"]
        + scored["expectation_score"] * effective_weights["expectation"]
    )
    scored = scored.sort_values("composite_score", ascending=False).reset_index(drop=True)
    scored["rank_value"] = scored.index + 1
    return scored


def select_stocks(frame: pd.DataFrame, top_n: int = 5, weights: dict[str, float] | None = None) -> pd.DataFrame:
    scored = build_composite_scores(frame, weights)
    result = scored.head(top_n)
    output_cols = ["code"]
    if "name" in result.columns:
        output_cols.append("name")
    output_cols += ["quality_score", "value_score", "expectation_score", "composite_score", "rank_value"]
    if "close" in result.columns:
        output_cols.append("close")
    return result[[c for c in output_cols if c in result.columns]]


def export_selection(selection: pd.DataFrame, output_path: Path) -> None:
    selection.to_csv(output_path, index=False, encoding="utf-8-sig")
