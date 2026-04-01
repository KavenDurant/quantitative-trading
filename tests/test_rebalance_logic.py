import pandas as pd

from qt.strategy.position_sizer import assign_target_shares
from qt.strategy.rebalancer import build_rebalance_signals


def test_build_rebalance_signals_creates_buy_orders():
    candidates = pd.DataFrame(
        [
            {"code": "600519", "target_weight": 0.5, "target_shares": 100, "last_price": 100.0},
            {"code": "000651", "target_weight": 0.5, "target_shares": 200, "last_price": 50.0},
        ]
    )

    signals = build_rebalance_signals(candidates, {})

    assert len(signals) == 2
    assert {signal.action for signal in signals} == {"BUY"}


def test_assign_target_shares_rounds_by_lot_size():
    candidates = pd.DataFrame([{"code": "600519", "last_price": 123.0}])
    sized = assign_target_shares(candidates, total_cash=20000, lot_size=100, cash_buffer_pct=0.0)
    assert sized.iloc[0]["target_shares"] % 100 == 0


def test_assign_target_shares_drops_zero_share_rows():
    candidates = pd.DataFrame([{"code": "600519", "last_price": 999999.0}])
    sized = assign_target_shares(candidates, total_cash=10000, lot_size=100, cash_buffer_pct=0.0)
    assert sized.empty
