from __future__ import annotations

import math

import pandas as pd


def compute_return(initial_cash: float, ending_nav: float) -> float:
    if initial_cash == 0:
        return 0.0
    return (ending_nav - initial_cash) / initial_cash


def compute_annualized_return(total_return: float, months: int) -> float:
    if months <= 0:
        return 0.0
    years = months / 12
    if total_return <= -1:
        return -1.0
    return (1 + total_return) ** (1 / years) - 1


def compute_max_drawdown(nav_series: pd.Series) -> float:
    if nav_series.empty:
        return 0.0
    rolling_peak = nav_series.cummax()
    drawdown = nav_series / rolling_peak - 1
    return float(drawdown.min())


def compute_sharpe_ratio(monthly_returns: pd.Series, risk_free_annual: float = 0.02) -> float:
    if monthly_returns.empty or len(monthly_returns) < 2:
        return 0.0
    rf_monthly = (1 + risk_free_annual) ** (1 / 12) - 1
    excess = monthly_returns - rf_monthly
    std = excess.std()
    if std == 0 or math.isnan(std):
        return 0.0
    return float((excess.mean() / std) * math.sqrt(12))


def compute_monthly_win_rate(monthly_returns: pd.Series) -> float:
    if monthly_returns.empty:
        return 0.0
    wins = (monthly_returns > 0).sum()
    return float(wins / len(monthly_returns))


def compute_monthly_returns(nav_series: pd.Series, calendar_month: bool = False) -> pd.Series:
    """计算收益率序列。

    默认保持原有行为：对输入序列做逐期 pct_change。
    当 calendar_month=True 时，要求输入带日期索引，并按月末净值重采样后计算月度收益率。
    """
    if len(nav_series) < 2:
        return pd.Series(dtype=float)

    if not calendar_month:
        return nav_series.pct_change().dropna()

    if isinstance(nav_series.index, pd.DatetimeIndex):
        dated_nav = nav_series.copy()
    else:
        try:
            dated_index = pd.to_datetime(nav_series.index)
        except (ValueError, TypeError):
            return pd.Series(dtype=float)
        if dated_index.isna().any():
            return pd.Series(dtype=float)
        dated_nav = nav_series.copy()
        dated_nav.index = dated_index

    monthly_nav = dated_nav.sort_index().resample("ME").last().dropna()
    if len(monthly_nav) < 2:
        return pd.Series(dtype=float)

    return monthly_nav.pct_change().dropna()


def compute_benchmark_comparison(
    strategy_nav: pd.Series,
    benchmark_nav: pd.Series,
) -> dict[str, float]:
    if strategy_nav.empty or benchmark_nav.empty:
        return {"strategy_return": 0.0, "benchmark_return": 0.0, "excess_return": 0.0}
    s_ret = (strategy_nav.iloc[-1] - strategy_nav.iloc[0]) / strategy_nav.iloc[0]
    b_ret = (benchmark_nav.iloc[-1] - benchmark_nav.iloc[0]) / benchmark_nav.iloc[0]
    return {
        "strategy_return": float(s_ret),
        "benchmark_return": float(b_ret),
        "excess_return": float(s_ret - b_ret),
    }
