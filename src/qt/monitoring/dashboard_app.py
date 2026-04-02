from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from qt.backtest.metrics import compute_max_drawdown
from qt.common.config import load_app_config
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient


def _build_backtest_comparison_frame(repository: Repository) -> pd.DataFrame:
    run_ids = repository.load_recent_backtest_run_ids(limit=2)
    if len(run_ids) < 2:
        return pd.DataFrame()

    current = repository.load_backtest_nav_by_run(run_ids[0])[ ["trade_date", "nav"] ].rename(columns={"nav": "latest_run"})
    previous = repository.load_backtest_nav_by_run(run_ids[1])[ ["trade_date", "nav"] ].rename(columns={"nav": "previous_run"})

    comparison = current.merge(previous, on="trade_date", how="inner")
    return comparison.sort_values("trade_date").reset_index(drop=True)


def build_dashboard_data(project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float | int | str]]:
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    with client.connect() as connection:
        repository = Repository(connection)
        nav = repository.load_latest_backtest_nav()
        positions = repository.load_latest_backtest_positions()
        trades = repository.load_latest_backtest_trades()
        comparison = _build_backtest_comparison_frame(repository)

    latest_nav = float(nav.iloc[-1]["nav"]) if not nav.empty else 0.0
    latest_cash = float(nav.iloc[-1]["cash"]) if not nav.empty else 0.0
    latest_trade_date = str(nav.iloc[-1]["trade_date"]) if not nav.empty else "暂无数据"
    total_return = (latest_nav - config.initial_cash) / config.initial_cash if config.initial_cash > 0 and latest_nav > 0 else 0.0
    max_drawdown = compute_max_drawdown(nav["nav"]) if not nav.empty else 0.0

    summary = {
        "cash": round(latest_cash, 2),
        "positions": int(len(positions)),
        "nav": round(latest_nav, 2),
        "initial_cash": round(float(config.initial_cash), 2),
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "latest_trade_date": latest_trade_date,
        "rebalances": int(nav["trade_date"].nunique()) if not nav.empty else 0,
    }
    return nav, positions, trades, comparison, summary


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;800&family=Noto+Sans+SC:wght@400;500;700&display=swap');

            :root {
                --bg: #07111f;
                --panel: rgba(10, 20, 37, 0.82);
                --panel-strong: rgba(14, 29, 51, 0.92);
                --border: rgba(117, 214, 255, 0.24);
                --text: #ebf7ff;
                --muted: #8da7c2;
                --cyan: #65e7ff;
                --emerald: #45f0b8;
                --amber: #ffbf69;
                --rose: #ff6b8a;
            }

            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(101, 231, 255, 0.10), transparent 28%),
                    radial-gradient(circle at top right, rgba(69, 240, 184, 0.08), transparent 24%),
                    linear-gradient(180deg, #07111f 0%, #091522 42%, #040912 100%);
                color: var(--text);
            }

            .main .block-container {
                max-width: 1400px;
                padding-top: 2.2rem;
                padding-bottom: 2rem;
            }

            h1, h2, h3 {
                color: var(--text);
                letter-spacing: 0.03em;
            }

            .hero-shell {
                position: relative;
                overflow: hidden;
                padding: 1.6rem 1.8rem;
                border: 1px solid var(--border);
                border-radius: 24px;
                background: linear-gradient(135deg, rgba(9, 19, 34, 0.96), rgba(12, 27, 48, 0.86));
                box-shadow: 0 24px 80px rgba(0, 0, 0, 0.34), inset 0 1px 0 rgba(255, 255, 255, 0.06);
                margin-bottom: 1.25rem;
            }

            .hero-shell::after {
                content: "";
                position: absolute;
                inset: 0;
                background: linear-gradient(90deg, transparent, rgba(101, 231, 255, 0.08), transparent);
                transform: translateX(-100%);
                animation: sweep 7s linear infinite;
            }

            @keyframes sweep {
                to { transform: translateX(100%); }
            }

            .hero-kicker {
                font-family: 'Orbitron', sans-serif;
                font-size: 0.78rem;
                text-transform: uppercase;
                color: var(--cyan);
                letter-spacing: 0.22em;
                margin-bottom: 0.6rem;
            }

            .hero-title {
                font-family: 'Orbitron', sans-serif;
                font-size: 2.3rem;
                font-weight: 800;
                line-height: 1.1;
                margin: 0;
                text-shadow: 0 0 18px rgba(101, 231, 255, 0.18);
            }

            .hero-subtitle {
                font-family: 'Noto Sans SC', sans-serif;
                color: var(--muted);
                margin-top: 0.75rem;
                font-size: 0.96rem;
                line-height: 1.7;
            }

            .hero-pills {
                display: flex;
                gap: 0.75rem;
                flex-wrap: wrap;
                margin-top: 1.15rem;
            }

            .hero-pill {
                border: 1px solid rgba(101, 231, 255, 0.22);
                background: rgba(101, 231, 255, 0.08);
                color: var(--text);
                border-radius: 999px;
                padding: 0.38rem 0.85rem;
                font-size: 0.85rem;
                font-family: 'Noto Sans SC', sans-serif;
            }

            .section-label {
                font-family: 'Orbitron', sans-serif;
                font-size: 0.88rem;
                text-transform: uppercase;
                color: var(--cyan);
                letter-spacing: 0.16em;
                margin: 1.2rem 0 0.65rem 0;
            }

            div[data-testid="stMetric"] {
                background: linear-gradient(180deg, rgba(13, 25, 44, 0.92), rgba(9, 19, 34, 0.92));
                border: 1px solid var(--border);
                border-radius: 20px;
                padding: 1rem 1rem 0.85rem 1rem;
                box-shadow: 0 12px 30px rgba(0, 0, 0, 0.24);
            }

            div[data-testid="stMetric"] label {
                color: var(--muted) !important;
                font-family: 'Noto Sans SC', sans-serif;
                font-size: 0.82rem !important;
                letter-spacing: 0.04em;
            }

            div[data-testid="stMetricValue"] {
                color: var(--text);
                font-family: 'Orbitron', sans-serif;
                font-size: 1.7rem;
            }

            div[data-testid="stMetricDelta"] {
                font-family: 'Noto Sans SC', sans-serif;
            }

            .panel-card {
                border: 1px solid var(--border);
                border-radius: 24px;
                background: linear-gradient(180deg, rgba(11, 21, 37, 0.94), rgba(8, 17, 30, 0.92));
                padding: 1rem 1.05rem 1.1rem;
                box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);
                margin-top: 0.35rem;
            }

            .panel-title {
                font-family: 'Orbitron', sans-serif;
                color: var(--text);
                font-size: 1rem;
                margin-bottom: 0.35rem;
            }

            .panel-subtitle {
                font-family: 'Noto Sans SC', sans-serif;
                color: var(--muted);
                font-size: 0.85rem;
                margin-bottom: 0.85rem;
            }

            .risk-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 0.7rem;
            }

            .risk-item {
                border: 1px solid rgba(255,255,255,0.06);
                border-radius: 16px;
                background: rgba(255,255,255,0.03);
                padding: 0.8rem 0.9rem;
            }

            .risk-item .label {
                color: var(--muted);
                font-size: 0.78rem;
                margin-bottom: 0.35rem;
                font-family: 'Noto Sans SC', sans-serif;
            }

            .risk-item .value {
                color: var(--text);
                font-family: 'Orbitron', sans-serif;
                font-size: 1.08rem;
            }

            div[data-testid="stDataFrame"] {
                border: 1px solid var(--border);
                border-radius: 18px;
                overflow: hidden;
                background: rgba(9, 19, 34, 0.82);
            }

            div[data-testid="stSidebar"] {
                background: linear-gradient(180deg, rgba(7, 17, 31, 0.98), rgba(7, 17, 31, 0.88));
                border-right: 1px solid rgba(101, 231, 255, 0.1);
            }

            .stAlert {
                border-radius: 16px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(summary: dict[str, float | int | str]) -> None:
    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-kicker">Quantitative Trading Command Deck</div>
            <h1 class="hero-title">A股基本面量化交易驾驶舱</h1>
            <div class="hero-subtitle">
                以真实回填数据和最新回测结果为核心，集中展示组合净值、风险阈值、持仓结构与交易活动，
                用更像“量化终端”的方式替代默认表格页面。
            </div>
            <div class="hero-pills">
                <span class="hero-pill">最新交易日：{summary['latest_trade_date']}</span>
                <span class="hero-pill">回测调仓次数：{summary['rebalances']}</span>
                <span class="hero-pill">当前持仓数：{summary['positions']}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_risk_panel(config, summary: dict[str, float | int | str]) -> None:
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">风控与组合边界</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-subtitle">保持核心参数可见，便于快速确认策略是否仍运行在既定纪律内。</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="risk-grid">
            <div class="risk-item"><div class="label">止损线</div><div class="value">{config.stop_loss_pct:.0%}</div></div>
            <div class="risk-item"><div class="label">止盈线 1</div><div class="value">{config.take_profit_1_pct:.0%}</div></div>
            <div class="risk-item"><div class="label">止盈线 2</div><div class="value">{config.take_profit_2_pct:.0%}</div></div>
            <div class="risk-item"><div class="label">月度组合止损</div><div class="value">{config.monthly_portfolio_stop_loss_pct:.0%}</div></div>
            <div class="risk-item"><div class="label">最大持仓数</div><div class="value">{config.max_positions}</div></div>
            <div class="risk-item"><div class="label">单只权重上限</div><div class="value">{config.max_single_position_weight:.0%}</div></div>
            <div class="risk-item"><div class="label">持仓上限天数</div><div class="value">{config.holding_period_days}天</div></div>
            <div class="risk-item"><div class="label">初始资金 / 当前净值</div><div class="value">{summary['initial_cash']:.0f} → {summary['nav']:.0f}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(page_title="A股基本面量化交易", page_icon="📈", layout="wide", initial_sidebar_state="expanded")
    inject_styles()

    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    nav, positions, trades, comparison, summary = build_dashboard_data(project_root)

    st.sidebar.markdown("### 控制台")
    auto_refresh = st.sidebar.checkbox("自动刷新（30秒）", value=False)
    st.sidebar.caption(f"数据源：{config.data_provider}")
    st.sidebar.caption(f"数据库：{config.db_path.name}")
    st.sidebar.caption(f"最新交易日：{summary['latest_trade_date']}")

    render_hero(summary)

    st.markdown('<div class="section-label">Portfolio pulse</div>', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("组合净值", f"{summary['nav']:.2f}")
    col2.metric("可用现金", f"{summary['cash']:.2f}")
    col3.metric("持仓数量", int(summary["positions"]))
    col4.metric("累计收益率", f"{float(summary['total_return']):.2%}", delta=f"最大回撤 {float(summary['max_drawdown']):.2%}")

    left_col, right_col = st.columns([1.75, 1], gap="large")

    with left_col:
        st.markdown('<div class="section-label">Equity curve</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">净值曲线</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-subtitle">基于最新回测运行结果，观察净值轨迹与策略波动。</div>', unsafe_allow_html=True)
        if not nav.empty:
            chart_frame = nav[["trade_date", "nav"]].copy().set_index("trade_date")
            st.area_chart(chart_frame, color="#65e7ff")
        else:
            st.info("暂无回测结果，请先运行 backfill_data 和 run_backtest")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="section-label">Backtest comparison</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">历史回测对比</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-subtitle">对比最近两次回测的共同区间净值走势，快速识别迭代后收益差异。</div>', unsafe_allow_html=True)
        if not comparison.empty:
            comparison_chart = comparison.set_index("trade_date")[["latest_run", "previous_run"]]
            st.line_chart(comparison_chart)
        else:
            st.info("历史回测对比需要至少两次回测记录")
        st.markdown('</div>', unsafe_allow_html=True)

    with right_col:
        st.markdown('<div class="section-label">Risk envelope</div>', unsafe_allow_html=True)
        render_risk_panel(config, summary)

    st.markdown('<div class="section-label">Live book</div>', unsafe_allow_html=True)
    pos_col, trade_col = st.columns(2, gap="large")

    with pos_col:
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">当前持仓</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-subtitle">最新回测时点的股票持仓与价格快照。</div>', unsafe_allow_html=True)
        st.dataframe(positions, use_container_width=True, height=340)
        st.markdown('</div>', unsafe_allow_html=True)

    with trade_col:
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">最近交易</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-subtitle">展示最近一次回测时点的交易记录。</div>', unsafe_allow_html=True)
        st.dataframe(trades, use_container_width=True, height=340)
        st.markdown('</div>', unsafe_allow_html=True)

    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
