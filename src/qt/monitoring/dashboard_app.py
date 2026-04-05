"""A股基本面量化交易看板 — 终端奢华风格

设计理念：深海军蓝 + 琥珀金色调，将专业量化终端的数据密度
与高端金融界面的精致感结合。Plotly 替代原生图表。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from qt.backtest.metrics import (
    compute_max_drawdown,
    compute_monthly_returns,
    compute_sharpe_ratio,
    compute_monthly_win_rate,
)
from qt.common.config import load_app_config
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient

# ── Plotly 全局暗色模板 ──────────────────────────────────────────

PLOTLY_TEMPLATE = "plotly_dark"

PLOTLY_LAYOUT = dict(
    template=PLOTLY_TEMPLATE,
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, Noto Sans SC, sans-serif", size=12, color="#c8d6e5"),
    margin=dict(l=40, r=24, t=48, b=36),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", font_size=11,
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
    ),
    xaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.08)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.08)"),
)

AMBER = "#f0a500"
AMBER_LIGHT = "#ffd166"
TEAL = "#2ec4b6"
ROSE = "#ff6b6b"
NAVY_DARK = "#0a0e27"
NAVY_MID = "#111738"
SILVER = "#c8d6e5"
MUTED = "#6b7b8d"

# ── 数据层 ────────────────────────────────────────────────────────


def _build_backtest_comparison_frame(repository: Repository) -> pd.DataFrame:
    run_ids = repository.load_recent_backtest_run_ids(limit=2)
    if len(run_ids) < 2:
        return pd.DataFrame()
    current = repository.load_backtest_nav_by_run(run_ids[0])[["trade_date", "nav"]].rename(columns={"nav": "最新回测"})
    previous = repository.load_backtest_nav_by_run(run_ids[1])[["trade_date", "nav"]].rename(columns={"nav": "上一次回测"})
    return current.merge(previous, on="trade_date", how="inner").sort_values("trade_date").reset_index(drop=True)


def _enrich_with_names(df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    if df.empty or "code" not in df.columns:
        return df
    result = df.copy()
    result["name"] = result["code"].map(name_map)
    cols = list(result.columns)
    if "name" in cols:
        cols.remove("name")
        cols.insert(cols.index("code") + 1, "name")
        result = result[cols]
    return result


def build_dashboard_data(
    project_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    with client.connect() as conn:
        repo = Repository(conn)
        nav = repo.load_latest_backtest_nav()
        positions = repo.load_latest_backtest_positions()
        trades = repo.load_latest_backtest_trades()
        comparison = _build_backtest_comparison_frame(repo)
        name_map = repo.load_stock_name_map()

    positions = _enrich_with_names(positions, name_map)
    trades = _enrich_with_names(trades, name_map)

    latest_nav = float(nav.iloc[-1]["nav"]) if not nav.empty else 0.0
    latest_cash = float(nav.iloc[-1]["cash"]) if not nav.empty else 0.0
    latest_date = str(nav.iloc[-1]["trade_date"]) if not nav.empty else "暂无"
    total_return = (latest_nav - config.initial_cash) / config.initial_cash if config.initial_cash > 0 and latest_nav > 0 else 0.0
    max_dd = compute_max_drawdown(nav["nav"]) if not nav.empty else 0.0
    monthly_rets = pd.Series(dtype=float)
    if not nav.empty:
        nav_for_monthly = nav.copy()
        nav_for_monthly["trade_date"] = pd.to_datetime(nav_for_monthly["trade_date"])
        monthly_rets = compute_monthly_returns(
            nav_for_monthly.set_index("trade_date")["nav"],
            calendar_month=True,
        )
    sharpe = compute_sharpe_ratio(monthly_rets) if not monthly_rets.empty else 0.0
    win_rate = compute_monthly_win_rate(monthly_rets) if not monthly_rets.empty else 0.0

    summary = dict(
        cash=round(latest_cash, 2),
        positions=int(len(positions)),
        nav=round(latest_nav, 2),
        initial_cash=round(float(config.initial_cash), 2),
        total_return=total_return,
        max_drawdown=max_dd,
        latest_date=latest_date,
        rebalances=int(nav["trade_date"].nunique()) if not nav.empty else 0,
        sharpe=round(sharpe, 2),
        win_rate=round(win_rate, 2),
        pnl=round(latest_nav - config.initial_cash, 2),
    )
    return nav, positions, trades, comparison, summary


# ── 样式注入 ──────────────────────────────────────────────────────

def inject_styles() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Noto+Sans+SC:wght@300;400;500;700&family=Playfair+Display:wght@700&display=swap');

            :root {
                --bg-deep: #060a1f;
                --bg-card: rgba(12, 18, 42, 0.88);
                --border: rgba(240, 165, 0, 0.12);
                --border-hover: rgba(240, 165, 0, 0.28);
                --text: #e0e6ed;
                --muted: #6b7b8d;
                --amber: #f0a500;
                --amber-soft: rgba(240, 165, 0, 0.10);
                --teal: #2ec4b6;
                --rose: #ff6b6b;
            }

            .stApp {
                background:
                    radial-gradient(ellipse at 20% 0%, rgba(240, 165, 0, 0.04), transparent 50%),
                    radial-gradient(ellipse at 80% 100%, rgba(46, 196, 182, 0.03), transparent 40%),
                    linear-gradient(175deg, #060a1f 0%, #0a0e27 40%, #080c20 100%);
                color: var(--text);
            }

            .main .block-container {
                max-width: 1440px;
                padding-top: 1.6rem;
                padding-bottom: 2rem;
            }

            h1, h2, h3 { color: var(--text); }

            /* ── Hero band ── */
            .hero-band {
                position: relative;
                padding: 1.4rem 1.6rem 1.2rem;
                border: 1px solid var(--border);
                border-radius: 20px;
                background: linear-gradient(135deg, rgba(10, 14, 39, 0.96), rgba(14, 22, 50, 0.80));
                margin-bottom: 1rem;
                overflow: hidden;
            }
            .hero-band::before {
                content: "";
                position: absolute;
                top: 0; left: 0; right: 0;
                height: 2px;
                background: linear-gradient(90deg, transparent, var(--amber), transparent);
                opacity: 0.6;
            }
            .hero-eyebrow {
                font-family: 'DM Mono', monospace;
                font-size: 0.72rem;
                color: var(--amber);
                letter-spacing: 0.2em;
                text-transform: uppercase;
                margin-bottom: 0.4rem;
            }
            .hero-heading {
                font-family: 'Noto Sans SC', sans-serif;
                font-weight: 700;
                font-size: 1.7rem;
                line-height: 1.25;
                margin: 0;
                color: var(--text);
            }
            .hero-desc {
                font-family: 'Noto Sans SC', sans-serif;
                color: var(--muted);
                font-size: 0.88rem;
                margin-top: 0.55rem;
                line-height: 1.65;
            }

            /* ── Stat tiles ── */
            .stat-row {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 0.65rem;
                margin-bottom: 1rem;
            }
            .stat-tile {
                border: 1px solid var(--border);
                border-radius: 16px;
                background: var(--bg-card);
                padding: 0.9rem 1rem;
                transition: border-color 0.2s;
            }
            .stat-tile:hover { border-color: var(--border-hover); }
            .stat-tile .label {
                font-family: 'Noto Sans SC', sans-serif;
                color: var(--muted);
                font-size: 0.78rem;
                margin-bottom: 0.3rem;
            }
            .stat-tile .value {
                font-family: 'DM Mono', monospace;
                font-size: 1.35rem;
                font-weight: 500;
                color: var(--text);
            }
            .stat-tile .value.positive { color: var(--teal); }
            .stat-tile .value.negative { color: var(--rose); }

            /* ── Section labels ── */
            .sec-label {
                font-family: 'DM Mono', monospace;
                font-size: 0.78rem;
                text-transform: uppercase;
                color: var(--amber);
                letter-spacing: 0.14em;
                margin: 1.3rem 0 0.5rem;
            }

            /* ── Panel card ── */
            .card {
                border: 1px solid var(--border);
                border-radius: 20px;
                background: var(--bg-card);
                padding: 1rem 1.1rem;
                margin-bottom: 0.75rem;
            }
            .card-title {
                font-family: 'Noto Sans SC', sans-serif;
                font-weight: 500;
                font-size: 0.95rem;
                color: var(--text);
                margin-bottom: 0.15rem;
            }
            .card-desc {
                font-family: 'Noto Sans SC', sans-serif;
                color: var(--muted);
                font-size: 0.82rem;
                margin-bottom: 0.7rem;
            }

            /* ── Risk grid ── */
            .risk-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 0.5rem;
            }
            .risk-cell {
                border: 1px solid rgba(255,255,255,0.04);
                border-radius: 12px;
                background: rgba(255,255,255,0.02);
                padding: 0.65rem 0.8rem;
            }
            .risk-cell .rl {
                color: var(--muted);
                font-size: 0.74rem;
                font-family: 'Noto Sans SC', sans-serif;
            }
            .risk-cell .rv {
                font-family: 'DM Mono', monospace;
                font-size: 1rem;
                color: var(--text);
                margin-top: 0.15rem;
            }

            /* ── Dataframe ── */
            div[data-testid="stDataFrame"] {
                border: 1px solid var(--border);
                border-radius: 14px;
                overflow: hidden;
                background: rgba(8, 12, 32, 0.80);
            }

            /* ── Sidebar ── */
            div[data-testid="stSidebar"] {
                background: linear-gradient(180deg, rgba(6, 10, 31, 0.98), rgba(6, 10, 31, 0.90));
                border-right: 1px solid var(--border);
            }

            /* ── Streamlit metrics override ── */
            div[data-testid="stMetric"] {
                background: var(--bg-card);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 0.85rem 0.9rem;
            }
            div[data-testid="stMetric"] label {
                color: var(--muted) !important;
                font-family: 'Noto Sans SC', sans-serif !important;
                font-size: 0.8rem !important;
            }
            div[data-testid="stMetricValue"] {
                color: var(--text);
                font-family: 'DM Mono', monospace;
            }

            .stAlert { border-radius: 14px; }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── 渲染组件 ──────────────────────────────────────────────────────


def render_hero(summary: dict) -> None:
    date_str = summary["latest_date"]
    rebalances = summary["rebalances"]
    pos_count = summary["positions"]

    pnl = summary["pnl"]
    pnl_class = "positive" if pnl >= 0 else "negative"
    pnl_sign = "+" if pnl >= 0 else ""

    st.markdown(
        f"""
        <div class="hero-band">
            <div class="hero-eyebrow">A-Share Fundamental Quant</div>
            <div class="hero-heading">A股基本面量化交易驾驶舱</div>
            <div class="hero-desc">
                集中展示回测净值曲线、风险边界、持仓明细与交易流水。
                每个数字都是策略运行的忠实记录 — 不猜测、不渲染，用数据说话。
            </div>
            <div style="display:flex;gap:0.6rem;flex-wrap:wrap;margin-top:0.85rem;">
                <span style="border:1px solid var(--border);background:var(--amber-soft);color:var(--text);border-radius:999px;padding:0.3rem 0.75rem;font-size:0.82rem;font-family:'DM Mono',monospace;">
                    {date_str}
                </span>
                <span style="border:1px solid var(--border);background:var(--amber-soft);color:var(--text);border-radius:999px;padding:0.3rem 0.75rem;font-size:0.82rem;font-family:'DM Mono',monospace;">
                    {rebalances} 次调仓
                </span>
                <span style="border:1px solid var(--border);background:var(--amber-soft);color:var(--text);border-radius:999px;padding:0.3rem 0.75rem;font-size:0.82rem;font-family:'DM Mono',monospace;">
                    {pos_count} 只持仓
                </span>
                <span style="border:1px solid var(--border);background:rgba(46,196,182,0.08);color:var(--text);border-radius:999px;padding:0.3rem 0.75rem;font-size:0.82rem;font-family:'DM Mono',monospace;">
                    P&L <span class="{pnl_class}">{pnl_sign}{pnl:,.0f}</span>
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_stat_row(summary: dict) -> None:
    """顶部关键指标行 — 用 HTML tile 而非 st.metric 以获得更紧凑的布局"""
    items = [
        ("组合净值", f"{summary['nav']:,.2f}", None),
        ("可用现金", f"{summary['cash']:,.2f}", None),
        ("持仓数量", str(summary["positions"]), None),
        ("累计收益率", f"{summary['total_return']:.1%}", "positive" if summary["total_return"] >= 0 else "negative"),
        ("最大回撤", f"{summary['max_drawdown']:.1%}", "negative"),
        ("夏普比率", f"{summary['sharpe']:.2f}", "positive" if summary["sharpe"] > 0 else "negative"),
        ("月度胜率", f"{summary['win_rate']:.0%}", "positive" if summary["win_rate"] >= 0.5 else "negative"),
    ]

    tiles = ""
    for label, value, cls in items:
        vcls = f" {cls}" if cls else ""
        tiles += f'<div class="stat-tile"><div class="label">{label}</div><div class="value{vcls}">{value}</div></div>'

    st.markdown(f'<div class="stat-row">{tiles}</div>', unsafe_allow_html=True)


def render_nav_chart(nav: pd.DataFrame, initial_cash: float) -> None:
    """净值曲线 — Plotly 面积图"""
    if nav.empty:
        st.info("暂无回测结果，请先运行 backfill_data 和 run_backtest")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=nav["trade_date"], y=nav["nav"],
            fill="tozeroy", fillcolor=f"rgba(240, 165, 0, 0.08)",
            line=dict(color=AMBER, width=2.2),
            name="组合净值",
            hovertemplate="%{x}<br>净值: %{y:,.2f}<extra></extra>",
        )
    )
    # 基准线 — 初始资金
    fig.add_hline(
        y=initial_cash,
        line_dash="dot", line_color="rgba(255,255,255,0.15)",
        annotation_text="初始资金", annotation_font_size=10,
        annotation_font_color=MUTED,
    )

    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="净值曲线", font=dict(size=14, color=SILVER), x=0, xanchor="left"),
        xaxis_title="", yaxis_title="净值 (元)",
        height=360,
    )
    fig.update_xaxes(type="category")  # treat dates as categories for clean display
    st.plotly_chart(fig, use_container_width=True)


def render_comparison_chart(comparison: pd.DataFrame) -> None:
    """历史回测对比 — Plotly 双线"""
    if comparison.empty:
        st.info("历史回测对比需要至少两次回测记录")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=comparison["trade_date"], y=comparison["最新回测"],
        line=dict(color=AMBER, width=2), name="最新回测",
        hovertemplate="%{x}<br>%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=comparison["trade_date"], y=comparison["上一次回测"],
        line=dict(color=MUTED, width=1.5, dash="dash"), name="上一次回测",
        hovertemplate="%{x}<br>%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="历史回测对比", font=dict(size=14, color=SILVER), x=0, xanchor="left"),
        height=300,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_drawdown_chart(nav: pd.DataFrame) -> None:
    """回撤曲线"""
    if nav.empty:
        return

    nav_series = nav["nav"]
    peak = nav_series.cummax()
    dd = nav_series / peak - 1

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nav["trade_date"], y=dd,
        fill="tozeroy", fillcolor=f"rgba(255, 107, 107, 0.12)",
        line=dict(color=ROSE, width=1.5), name="回撤",
        hovertemplate="%{x}<br>回撤: %{y:.1%}<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="回撤曲线", font=dict(size=14, color=SILVER), x=0, xanchor="left"),
        height=280, yaxis_tickformat=".0%",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_risk_panel(config, summary: dict) -> None:
    """风控参数面板"""
    items = [
        ("止损线", f"{config.stop_loss_pct:.0%}"),
        ("止盈线 1", f"{config.take_profit_1_pct:.0%}"),
        ("止盈线 2", f"{config.take_profit_2_pct:.0%}"),
        ("月度组合止损", f"{config.monthly_portfolio_stop_loss_pct:.0%}"),
        ("最大持仓数", str(config.max_positions)),
        ("单只权重上限", f"{config.max_single_position_weight:.0%}"),
        ("持仓上限天数", f"{config.holding_period_days} 天"),
        ("初始 → 当前", f"{summary['initial_cash']:,.0f} → {summary['nav']:,.0f}"),
    ]
    cells = ""
    for label, value in items:
        cells += f'<div class="risk-cell"><div class="rl">{label}</div><div class="rv">{value}</div></div>'

    st.markdown(f'<div class="risk-grid">{cells}</div>', unsafe_allow_html=True)


# ── 主入口 ────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="A股量化交易驾驶舱",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_styles()

    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    nav, positions, trades, comparison, summary = build_dashboard_data(project_root)

    # ── 侧边栏 ──
    st.sidebar.markdown("### 控制台")
    auto_refresh = st.sidebar.checkbox("自动刷新 (30s)", value=False)
    st.sidebar.caption(f"数据源: {config.data_provider}")
    st.sidebar.caption(f"数据库: {config.db_path.name}")
    st.sidebar.caption(f"最新交易日: {summary['latest_date']}")
    st.sidebar.divider()
    st.sidebar.markdown(
        "**策略说明**\n\n"
        "本策略基于 **质量 + 估值 + 预期** 三因子模型，\n"
        "每月调仓一次，最多持有 5 只主板股票。\n\n"
        "- 质量因子: ROE、毛利率、现金流质量\n"
        "- 估值因子: PE、PB、PS 行业排名\n"
        "- 预期因子: 净利润增速、营收增速\n\n"
        "止损 -8%，止盈 +15%/+25%，\n"
        "持仓上限 120 天。"
    )

    # ── 主内容 ──
    render_hero(summary)
    render_stat_row(summary)

    # 上半区：净值曲线 + 回撤 + 风控
    st.markdown('<div class="sec-label">Performance</div>', unsafe_allow_html=True)
    left_col, right_col = st.columns([2, 1], gap="medium")

    with left_col:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        render_nav_chart(nav, summary["initial_cash"])
        st.markdown('</div>', unsafe_allow_html=True)

        # 回测对比
        if not comparison.empty:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            render_comparison_chart(comparison)
            st.markdown('</div>', unsafe_allow_html=True)

    with right_col:
        # 回撤
        st.markdown('<div class="card">', unsafe_allow_html=True)
        render_drawdown_chart(nav)
        st.markdown('</div>', unsafe_allow_html=True)

        # 风控面板
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">风控边界</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="card-desc">核心参数一览，确认策略始终在纪律范围内运行。</div>',
            unsafe_allow_html=True,
        )
        render_risk_panel(config, summary)
        st.markdown('</div>', unsafe_allow_html=True)

    # 下半区：持仓 + 交易
    st.markdown('<div class="sec-label">Portfolio</div>', unsafe_allow_html=True)
    pos_col, trade_col = st.columns(2, gap="medium")

    with pos_col:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">当前持仓</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-desc">最新回测时点的股票持仓快照。</div>', unsafe_allow_html=True)
        st.dataframe(positions, use_container_width=True, height=320)
        st.markdown('</div>', unsafe_allow_html=True)

    with trade_col:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">最近交易</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-desc">最新回测时点的买卖记录。</div>', unsafe_allow_html=True)
        st.dataframe(trades, use_container_width=True, height=320)
        st.markdown('</div>', unsafe_allow_html=True)

    # 自动刷新
    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
