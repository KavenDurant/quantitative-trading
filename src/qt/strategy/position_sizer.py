from __future__ import annotations

import math

import pandas as pd


def assign_target_shares(
    candidates: pd.DataFrame,
    total_cash: float,
    lot_size: int,
    cash_buffer_pct: float,
    max_single_position_weight: float = 0.2,
) -> pd.DataFrame:
    sized = candidates.copy()
    if sized.empty:
        sized["target_weight"] = []
        sized["target_shares"] = []
        return sized

    investable_cash = total_cash * (1 - cash_buffer_pct)
    n = len(sized)
    target_weight = min(1 / n, max_single_position_weight)
    sized["target_weight"] = target_weight
    sized["target_shares"] = sized["last_price"].apply(
        lambda price: max(math.floor((investable_cash * target_weight) / price / lot_size) * lot_size, 0)
    )
    sized = sized[sized["target_shares"] > 0].reset_index(drop=True)
    if sized.empty:
        return sized
    sized["target_weight"] = 1 / len(sized)
    return sized


def compute_buy_limit_price(close: float, slippage_rate: float = 0.005) -> float:
    return round(close * (1 + slippage_rate), 2)


def compute_stop_loss_price(buy_price: float, stop_loss_pct: float = -0.08) -> float:
    return round(buy_price * (1 + stop_loss_pct), 2)


def compute_take_profit_price(buy_price: float, take_profit_pct: float = 0.15) -> float:
    return round(buy_price * (1 + take_profit_pct), 2)


def build_position_table(
    candidates: pd.DataFrame,
    total_cash: float,
    lot_size: int,
    cash_buffer_pct: float,
    slippage_rate: float = 0.005,
    stop_loss_pct: float = -0.08,
    take_profit_1_pct: float = 0.15,
    take_profit_2_pct: float = 0.25,
    max_single_position_weight: float = 0.2,
) -> pd.DataFrame:
    sized = assign_target_shares(candidates, total_cash, lot_size, cash_buffer_pct, max_single_position_weight)
    if sized.empty:
        return sized
    sized["buy_limit"] = sized["last_price"].apply(lambda p: compute_buy_limit_price(p, slippage_rate))
    sized["stop_loss"] = sized["buy_limit"].apply(lambda p: compute_stop_loss_price(p, stop_loss_pct))
    sized["take_profit_1"] = sized["buy_limit"].apply(lambda p: compute_take_profit_price(p, take_profit_1_pct))
    sized["take_profit_2"] = sized["buy_limit"].apply(lambda p: compute_take_profit_price(p, take_profit_2_pct))
    sized["position_amount"] = sized["target_shares"] * sized["buy_limit"]
    return sized
