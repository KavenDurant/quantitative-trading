from __future__ import annotations

from datetime import datetime


def cap_position_count(requested: int, max_positions: int) -> int:
    return min(requested, max_positions)


def check_stop_loss(current_price: float, buy_price: float, stop_loss_pct: float = -0.08) -> bool:
    if buy_price <= 0:
        return False
    return (current_price - buy_price) / buy_price <= stop_loss_pct


def check_take_profit(current_price: float, buy_price: float, take_profit_pct: float = 0.15) -> bool:
    if buy_price <= 0:
        return False
    return (current_price - buy_price) / buy_price >= take_profit_pct


def check_holding_period(buy_date: str, current_date: str, max_days: int = 120) -> bool:
    try:
        bd = datetime.strptime(buy_date, "%Y-%m-%d")
        cd = datetime.strptime(current_date, "%Y-%m-%d")
        return (cd - bd).days >= max_days
    except (ValueError, TypeError):
        return False


def check_portfolio_stop_loss(
    current_nav: float, month_start_nav: float, threshold: float = -0.12
) -> bool:
    if month_start_nav <= 0:
        return False
    return (current_nav - month_start_nav) / month_start_nav <= threshold


def check_single_position_weight(
    position_value: float, total_nav: float, max_weight: float = 0.2
) -> bool:
    if total_nav <= 0:
        return False
    return position_value / total_nav > max_weight


def check_market_trend(
    prices: list[float],
    short_window: int = 20,
    long_window: int = 60,
) -> bool:
    if long_window <= 0 or short_window <= 0 or short_window > long_window:
        return False
    if len(prices) < long_window:
        return False

    latest = prices[-1]
    short_ma = sum(prices[-short_window:]) / short_window
    long_ma = sum(prices[-long_window:]) / long_window
    return latest >= long_ma and short_ma >= long_ma
