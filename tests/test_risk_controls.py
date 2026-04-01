from qt.strategy.risk_controls import cap_position_count


def test_cap_position_count_respects_max():
    assert cap_position_count(10, 5) == 5
    assert cap_position_count(3, 5) == 3
