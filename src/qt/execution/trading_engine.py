from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from qt.common.config import AppConfig, load_app_config
from qt.common.logger import get_logger
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient
from qt.execution.order_manager import OrderManager
from qt.execution.paper_broker import PaperBroker
from qt.factors.combiner import build_composite_scores
from qt.monitoring.notifier import Notifier
from qt.strategy.position_sizer import build_position_table
from qt.strategy.rebalancer import build_rebalance_signals
from qt.strategy.risk_controls import (
    check_holding_period,
    check_market_trend,
    check_portfolio_stop_loss,
    check_single_position_weight,
    check_stop_loss,
    check_take_profit,
)
from qt.strategy.selector import select_top_candidates

logger = get_logger(__name__)


class TradingEngine:
    def __init__(self, config: AppConfig, broker: PaperBroker, repository: Repository) -> None:
        self.config = config
        self.broker = broker
        self.repository = repository
        self.notifier = Notifier()
        self.order_manager = OrderManager(broker)
        self.month_start_nav: float = config.initial_cash

    def run_rebalance(self, as_of_date: str) -> None:
        logger.info("开始月度调仓 date=%s", as_of_date)
        fundamentals = self.repository.load_latest_fundamentals(as_of_date)
        prices = self.repository.load_prices_for_date(as_of_date)
        if fundamentals.empty or prices.empty:
            logger.warning("数据不足，跳过调仓")
            return

        if self.config.market_timing_enabled:
            long_window = max(self.config.market_timing_long_window, 1)
            benchmark_code = self.config.benchmark.split(".")[0]
            benchmark_window = self.repository.load_recent_prices(benchmark_code, as_of_date, long_window)
            if len(benchmark_window) < long_window:
                logger.warning(
                    "基准指数数据不足，使用全市场均价作为大盘趋势代理 benchmark=%s points=%d",
                    self.config.benchmark,
                    len(benchmark_window),
                )
                market_prices = self.repository.load_market_proxy_prices(as_of_date, long_window)
            else:
                market_prices = benchmark_window["close"].tolist()

            market_prices = [float(p) for p in market_prices]
            if len(market_prices) < long_window:
                logger.warning(
                    "大盘趋势代理数据不足，跳过本次调仓 date=%s required=%d actual=%d",
                    as_of_date,
                    long_window,
                    len(market_prices),
                )
                return

            market_trend_ok = check_market_trend(
                market_prices,
                short_window=self.config.market_timing_short_window,
                long_window=long_window,
            )
            if not market_trend_ok:
                logger.warning("大盘趋势不满足开仓条件，跳过本次调仓 date=%s benchmark=%s", as_of_date, self.config.benchmark)
                self.notifier.send_risk_alert("大盘趋势择时", f"{as_of_date} 大盘趋势不满足开仓条件，本次月度调仓跳过")
                return

        frame = fundamentals.merge(prices[["code", "close"]], on="code", how="inner")
        frame = frame.rename(columns={"close": "last_price"})
        scored = build_composite_scores(frame, self.config.factor_weights)
        selected = select_top_candidates(scored, self.config.max_positions)

        table = build_position_table(
            selected, self.broker.cash() + self._positions_value(prices),
            self.config.lot_size, self.config.cash_buffer_pct,
            self.config.slippage_rate, self.config.stop_loss_pct,
            self.config.take_profit_1_pct, self.config.take_profit_2_pct,
            self.config.max_single_position_weight,
        )

        signals = build_rebalance_signals(table, self.broker.positions())
        trades = self.order_manager.execute(as_of_date, signals)

        for code, side, shares, price, amount in trades:
            self.notifier.send_trade_alert(side, code, shares, price)
            logger.info("交易: %s %s %d @ %.2f = %.2f", side, code, shares, price, amount)

        self.month_start_nav = self.broker.cash() + self._positions_value(prices)
        logger.info("调仓完成 nav=%.2f", self.month_start_nav)

    def run_risk_check(self, as_of_date: str) -> list[str]:
        alerts: list[str] = []
        prices = self.repository.load_prices_for_date(as_of_date)
        price_map = dict(zip(prices["code"], prices["close"])) if not prices.empty else {}
        current_nav = self.broker.cash() + sum(
            price_map.get(code, 0) * shares for code, shares in self.broker.positions().items()
        )

        if check_portfolio_stop_loss(current_nav, self.month_start_nav, self.config.monthly_portfolio_stop_loss_pct):
            msg = f"组合月度止损触发: NAV={current_nav:.2f} 月初={self.month_start_nav:.2f}"
            alerts.append(msg)
            self.notifier.send_risk_alert("组合月度止损", msg)

        for code, shares in self.broker.positions().items():
            current_price = price_map.get(code, 0)
            buy_price = self.broker.avg_costs.get(code, 0)
            if buy_price <= 0:
                continue

            if check_stop_loss(current_price, buy_price, self.config.stop_loss_pct):
                msg = f"个股止损: {code} 现价={current_price:.2f} 成本={buy_price:.2f}"
                alerts.append(msg)
                self.notifier.send_risk_alert("个股止损", msg)

            if check_take_profit(current_price, buy_price, self.config.take_profit_1_pct):
                msg = f"个股止盈1: {code} 现价={current_price:.2f} 成本={buy_price:.2f}"
                alerts.append(msg)

            if check_single_position_weight(current_price * shares, current_nav, self.config.max_single_position_weight):
                msg = f"集中度超限: {code} 权重={current_price * shares / current_nav:.2%}"
                alerts.append(msg)

        return alerts

    def run_close_check(self, as_of_date: str) -> None:
        prices = self.repository.load_prices_for_date(as_of_date)
        price_map = dict(zip(prices["code"], prices["close"])) if not prices.empty else {}
        nav = self.broker.cash() + sum(
            price_map.get(code, 0) * shares for code, shares in self.broker.positions().items()
        )
        pnl_pct = (nav - self.config.initial_cash) / self.config.initial_cash if self.config.initial_cash > 0 else 0
        self.notifier.send_daily_summary(nav, self.broker.cash(), len(self.broker.positions()), pnl_pct)
        logger.info("收盘检查 nav=%.2f cash=%.2f positions=%d", nav, self.broker.cash(), len(self.broker.positions()))

    def _positions_value(self, prices: pd.DataFrame) -> float:
        price_map = dict(zip(prices["code"], prices["close"])) if not prices.empty else {}
        return sum(price_map.get(code, 0) * shares for code, shares in self.broker.positions().items())


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    client.init_db()
    today = date.today().isoformat()

    with client.connect() as connection:
        repository = Repository(connection)
        broker = PaperBroker(config.initial_cash, config.commission_rate, config.slippage_rate)
        engine = TradingEngine(config, broker, repository)
        engine.run_rebalance(today)
        engine.run_risk_check(today)
        engine.run_close_check(today)


if __name__ == "__main__":
    main()
