import pandas as pd

from qt.factors.combiner import build_composite_scores


def test_build_composite_scores_ranks_rows():
    frame = pd.DataFrame(
        [
            {"code": "A", "roe": 0.3, "gross_margin": 0.5, "operating_cashflow_ratio": 0.6, "pe_ttm": 10, "pb": 1, "ps_ttm": 2, "net_profit_yoy": 0.2, "revenue_yoy": 0.15},
            {"code": "B", "roe": 0.1, "gross_margin": 0.2, "operating_cashflow_ratio": 0.2, "pe_ttm": 30, "pb": 5, "ps_ttm": 6, "net_profit_yoy": 0.05, "revenue_yoy": 0.04},
        ]
    )

    scored = build_composite_scores(frame)

    assert list(scored["code"]) == ["A", "B"]
    assert list(scored["rank_value"]) == [1, 2]
