from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from qt.common.config import load_app_config
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient


def build_dashboard_data(project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float | int]]:
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    with client.connect() as connection:
        repository = Repository(connection)
        nav = repository.load_latest_backtest_nav()
        positions = repository.load_latest_backtest_positions()
        trades = repository.load_latest_backtest_trades()
    summary = {
        "cash": round(float(nav.iloc[-1]["cash"]), 2) if not nav.empty else 0.0,
        "positions": int(len(positions)),
        "nav": round(float(nav.iloc[-1]["nav"]), 2) if not nav.empty else 0.0,
    }
    return nav, positions, trades, summary


def main() -> None:
    st.set_page_config(page_title="A股基本面量化交易", layout="wide")
    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    nav, positions, trades, summary = build_dashboard_data(project_root)

    st.title("A股基本面量化交易 MVP")

    # 核心指标
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("组合净值", f"{summary['nav']:.2f}")
    col2.metric("可用现金", f"{summary['cash']:.2f}")
    col3.metric("持仓数量", summary["positions"])
    total_return = (summary["nav"] - config.initial_cash) / config.initial_cash if config.initial_cash > 0 else 0
    col4.metric("总收益率", f"{total_return:.2%}")

    # 风险参数
    st.subheader("风险参数")
    risk_col1, risk_col2, risk_col3, risk_col4 = st.columns(4)
    risk_col1.metric("止损线", f"{config.stop_loss_pct:.0%}")
    risk_col2.metric("止盈线1", f"{config.take_profit_1_pct:.0%}")
    risk_col3.metric("止盈线2", f"{config.take_profit_2_pct:.0%}")
    risk_col4.metric("组合月度止损", f"{config.monthly_portfolio_stop_loss_pct:.0%}")

    param_col1, param_col2, param_col3, param_col4 = st.columns(4)
    param_col1.metric("最大持仓数", config.max_positions)
    param_col2.metric("单只权重上限", f"{config.max_single_position_weight:.0%}")
    param_col3.metric("持仓上限天数", f"{config.holding_period_days}天")
    param_col4.metric("佣金率", f"{config.commission_rate:.4%}")

    # 净值曲线
    st.subheader("净值曲线")
    if not nav.empty:
        st.line_chart(nav.set_index("trade_date")["nav"])
    else:
        st.info("暂无回测结果，请先运行 backfill_data 和 run_backtest")

    # 当前持仓
    st.subheader("当前持仓")
    st.dataframe(positions, use_container_width=True)

    # 最近交易
    st.subheader("最近交易")
    st.dataframe(trades, use_container_width=True)

    # 自动刷新
    st.markdown("---")
    auto_refresh = st.checkbox("自动刷新 (30秒)", value=False)
    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
