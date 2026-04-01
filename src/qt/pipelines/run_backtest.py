from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pandas as pd

from qt.backtest.engine import compute_nav
from qt.backtest.metrics import compute_max_drawdown
from qt.backtest.report import build_full_report, save_nav_chart
from qt.common.calendar import month_end_dates
from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient
from qt.execution.order_manager import OrderManager
from qt.execution.paper_broker import PaperBroker
from qt.factors.combiner import build_composite_scores
from qt.strategy.position_sizer import assign_target_shares
from qt.strategy.rebalancer import build_rebalance_signals
from qt.strategy.risk_controls import check_stop_loss
from qt.strategy.selector import select_top_candidates

logger = get_logger(__name__)


def run_backtest(
    project_root: Path,
    factor_weights: dict[str, float] | None = None,
    max_positions: int | None = None,
    stop_loss_pct: float | None = None,
) -> dict:
    config = load_app_config(project_root)
    weights = factor_weights or config.factor_weights
    positions_limit = max_positions or config.max_positions
    sl_pct = stop_loss_pct if stop_loss_pct is not None else config.stop_loss_pct

    client = SQLiteClient(config.db_path)
    run_id = uuid4().hex[:12]
    rebalance_dates = month_end_dates(config.backtest_start, config.backtest_end)
    broker = PaperBroker(
        initial_cash=config.initial_cash,
        commission_rate=config.commission_rate,
        slippage_rate=config.slippage_rate,
    )
    nav_rows: list[tuple[str, float]] = []

    with client.connect() as connection:
        repository = Repository(connection)
        repository.save_backtest_run(run_id, config.data_provider, config.backtest_start, config.backtest_end)

        for trade_date in rebalance_dates:
            fundamentals = repository.load_latest_fundamentals(trade_date)
            prices = repository.load_prices_for_date(trade_date)
            if fundamentals.empty or prices.empty:
                continue

            frame = fundamentals.merge(prices[["code", "close"]], on="code", how="inner")
            frame = frame.rename(columns={"close": "last_price"})
            if frame.empty:
                continue

            # 日度止损检查
            price_map = dict(zip(prices["code"], prices["close"]))
            for code, shares in list(broker.positions().items()):
                current_price = price_map.get(code, 0)
                buy_price = broker.avg_costs.get(code, 0)
                if buy_price > 0 and check_stop_loss(current_price, buy_price, sl_pct):
                    broker.apply_orders(f"sl_{trade_date}_{code}", [(code, 0, current_price)])

            scored = build_composite_scores(frame, weights)
            selected = select_top_candidates(scored, positions_limit)
            targets = assign_target_shares(selected, broker.cash(), config.lot_size, config.cash_buffer_pct)
            signals = build_rebalance_signals(targets, broker.positions())
            trades = OrderManager(broker).execute(trade_date, signals)
            nav = compute_nav(broker.cash(), broker.positions(), price_map)
            nav_rows.append((trade_date, nav))
            repository.save_backtest_nav(run_id, trade_date, broker.cash(), nav)
            repository.save_backtest_positions(run_id, trade_date, broker.positions(), price_map)
            repository.save_backtest_trades(run_id, trade_date, trades)

    nav_series = pd.Series([v for _, v in nav_rows])
    dates = [d for d, _ in nav_rows]
    max_dd = compute_max_drawdown(nav_series)

    report = build_full_report(
        config.initial_cash, nav_series,
        max_drawdown=max_dd,
        positions_count=len(broker.positions()),
        rebalances=len(nav_rows),
    )
    report["run_id"] = run_id
    report["factor_weights"] = str(weights)
    report["max_positions"] = positions_limit
    report["stop_loss_pct"] = sl_pct

    # 保存净值曲线图
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)
    save_nav_chart(nav_series, dates, output_dir / f"nav_curve_{run_id}.png")

    logger.info("回测完成: %s", report)
    return report


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    report = run_backtest(project_root)
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
