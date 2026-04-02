from __future__ import annotations

DEFAULT_FACTOR_WEIGHTS = {
    "quality": 0.4,
    "value": 0.35,
    "expectation": 0.25,
}

ML_FALLBACK_FACTOR_WEIGHTS = {
    "quality_score": DEFAULT_FACTOR_WEIGHTS["quality"],
    "value_score": DEFAULT_FACTOR_WEIGHTS["value"],
    "expectation_score": DEFAULT_FACTOR_WEIGHTS["expectation"],
}
