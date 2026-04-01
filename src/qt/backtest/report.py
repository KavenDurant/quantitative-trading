from __future__ import annotations

from pathlib import Path

import pandas as pd

from qt.backtest.metrics import (
    compute_annualized_return,
    compute_benchmark_comparison,
    compute_monthly_returns,
    compute_monthly_win_rate,
    compute_sharpe_ratio,
)


def build_summary(
    initial_cash: float,
    ending_nav: float,
    positions_count: int,
    rebalances: int,
    max_drawdown: float,
) -> dict[str, float | int]:
    return {
        "initial_cash": initial_cash,
        "ending_nav": ending_nav,
        "positions_count": positions_count,
        "rebalances": rebalances,
        "max_drawdown": max_drawdown,
    }


def build_full_report(
    initial_cash: float,
    nav_series: pd.Series,
    benchmark_nav: pd.Series | None = None,
    max_drawdown: float = 0.0,
    positions_count: int = 0,
    rebalances: int = 0,
) -> dict[str, float | int | str]:
    ending_nav = float(nav_series.iloc[-1]) if not nav_series.empty else initial_cash
    total_return = (ending_nav - initial_cash) / initial_cash if initial_cash > 0 else 0.0
    months = len(nav_series)
    monthly_rets = compute_monthly_returns(nav_series)

    report: dict[str, float | int | str] = {
        "initial_cash": initial_cash,
        "ending_nav": round(ending_nav, 2),
        "total_return": round(total_return, 4),
        "annualized_return": round(compute_annualized_return(total_return, months), 4),
        "max_drawdown": round(max_drawdown, 4),
        "sharpe_ratio": round(compute_sharpe_ratio(monthly_rets), 4),
        "monthly_win_rate": round(compute_monthly_win_rate(monthly_rets), 4),
        "rebalances": rebalances,
        "positions_count": positions_count,
    }

    if benchmark_nav is not None and not benchmark_nav.empty:
        comparison = compute_benchmark_comparison(nav_series, benchmark_nav)
        report["benchmark_return"] = round(comparison["benchmark_return"], 4)
        report["excess_return"] = round(comparison["excess_return"], 4)

    return report


def save_nav_chart(nav_series: pd.Series, dates: list[str], output_path: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dates, nav_series.values, label="策略净值", linewidth=2)
        ax.set_xlabel("日期")
        ax.set_ylabel("净值")
        ax.set_title("回测净值曲线")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
    except ImportError:
        pass
