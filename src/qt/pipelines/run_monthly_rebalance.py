from __future__ import annotations

from pathlib import Path

import pandas as pd

from qt.backtest.engine import compute_nav
from qt.backtest.metrics import compute_return
from qt.common.config import load_app_config
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient
from qt.execution.order_manager import OrderManager
from qt.execution.paper_broker import PaperBroker
from qt.factors.combiner import build_composite_scores
from qt.strategy.position_sizer import assign_target_shares
from qt.strategy.rebalancer import build_rebalance_signals
from qt.strategy.selector import select_top_candidates


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)

    with client.connect() as connection:
        repository = Repository(connection)
        fundamentals = repository.load_latest_fundamentals(config.data_as_of)
        prices = repository.load_prices_for_date(config.data_as_of)

    if fundamentals.empty or prices.empty:
        print({"error": "no data available, run backfill first"})
        return

    frame = fundamentals.merge(prices[["code", "close"]], on="code", how="inner")
    frame = frame.rename(columns={"close": "last_price"})
    scored = build_composite_scores(frame, config.factor_weights)
    selected = select_top_candidates(scored, config.max_positions)
    targets = assign_target_shares(selected, config.initial_cash, config.lot_size, config.cash_buffer_pct)
    broker = PaperBroker(config.initial_cash, config.commission_rate, config.slippage_rate)
    signals = build_rebalance_signals(targets, broker.positions())
    OrderManager(broker).execute(config.data_as_of, signals)
    price_map = dict(zip(prices["code"], prices["close"]))
    ending_nav = compute_nav(broker.cash(), broker.positions(), price_map)
    print({"ending_nav": ending_nav, "return": compute_return(config.initial_cash, ending_nav)})


if __name__ == "__main__":
    main()
