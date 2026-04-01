from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import filter_by_liquidity, filter_by_price, listing_days
from qt.strategy.risk_controls import (
    check_holding_period,
    check_portfolio_stop_loss,
    check_single_position_weight,
    check_stop_loss,
    check_take_profit,
)

logger = get_logger(__name__)


@dataclass
class RiskCheckResult:
    code: str
    alert_type: str
    message: str
    action: str = ""


class RiskManager:
    def __init__(
        self,
        stop_loss_pct: float = -0.08,
        take_profit_1_pct: float = 0.15,
        take_profit_2_pct: float = 0.25,
        holding_period_days: int = 120,
        monthly_portfolio_stop_loss_pct: float = -0.12,
        max_single_position_weight: float = 0.2,
        min_price: float = 5.0,
        max_price: float = 50.0,
        min_daily_turnover: float = 30_000_000,
        min_listing_days: int = 365,
        blacklist: set[str] | None = None,
    ) -> None:
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_1_pct = take_profit_1_pct
        self.take_profit_2_pct = take_profit_2_pct
        self.holding_period_days = holding_period_days
        self.monthly_portfolio_stop_loss_pct = monthly_portfolio_stop_loss_pct
        self.max_single_position_weight = max_single_position_weight
        self.min_price = min_price
        self.max_price = max_price
        self.min_daily_turnover = min_daily_turnover
        self.min_listing_days = min_listing_days
        self.blacklist = blacklist or set()

    def pre_trade_filter(
        self,
        candidates: pd.DataFrame,
        as_of_date: str,
    ) -> pd.DataFrame:
        filtered = candidates.copy()

        # 价格过滤
        if "last_price" in filtered.columns:
            price_ok = filter_by_price(
                dict(zip(filtered["code"], filtered["last_price"])),
                self.min_price, self.max_price,
            )
            filtered = filtered[filtered["code"].isin(price_ok)]

        # 流动性过滤
        if "amount" in filtered.columns:
            liq_ok = filter_by_liquidity(
                dict(zip(filtered["code"], filtered["amount"])),
                self.min_daily_turnover,
            )
            filtered = filtered[filtered["code"].isin(liq_ok)]

        # 上市时间过滤
        if "list_date" in filtered.columns:
            filtered = filtered[
                filtered["list_date"].apply(lambda ld: listing_days(ld, as_of_date) >= self.min_listing_days)
            ]

        # ST / 黑名单过滤
        if "is_st" in filtered.columns:
            filtered = filtered[filtered["is_st"] == 0]
        filtered = filtered[~filtered["code"].isin(self.blacklist)]

        logger.info("风控前置过滤: %d -> %d", len(candidates), len(filtered))
        return filtered.reset_index(drop=True)

    def check_positions(
        self,
        positions: dict[str, int],
        avg_costs: dict[str, float],
        current_prices: dict[str, float],
        buy_dates: dict[str, str],
        current_date: str,
        current_nav: float,
        month_start_nav: float,
    ) -> list[RiskCheckResult]:
        alerts: list[RiskCheckResult] = []

        # 组合月度止损
        if check_portfolio_stop_loss(current_nav, month_start_nav, self.monthly_portfolio_stop_loss_pct):
            alerts.append(RiskCheckResult(
                code="PORTFOLIO", alert_type="月度止损",
                message=f"组合NAV={current_nav:.2f} 月初={month_start_nav:.2f}",
                action="清仓",
            ))

        for code, shares in positions.items():
            price = current_prices.get(code, 0)
            cost = avg_costs.get(code, 0)
            if cost <= 0:
                continue

            # 个股止损
            if check_stop_loss(price, cost, self.stop_loss_pct):
                alerts.append(RiskCheckResult(
                    code=code, alert_type="个股止损",
                    message=f"现价={price:.2f} 成本={cost:.2f} 跌幅={(price-cost)/cost:.2%}",
                    action="卖出",
                ))

            # 止盈1
            if check_take_profit(price, cost, self.take_profit_1_pct):
                alerts.append(RiskCheckResult(
                    code=code, alert_type="止盈1",
                    message=f"现价={price:.2f} 成本={cost:.2f} 涨幅={(price-cost)/cost:.2%}",
                    action="减仓50%",
                ))

            # 止盈2
            if check_take_profit(price, cost, self.take_profit_2_pct):
                alerts.append(RiskCheckResult(
                    code=code, alert_type="止盈2",
                    message=f"现价={price:.2f} 成本={cost:.2f} 涨幅={(price-cost)/cost:.2%}",
                    action="全部卖出",
                ))

            # 持仓超期
            buy_date = buy_dates.get(code, "")
            if buy_date and check_holding_period(buy_date, current_date, self.holding_period_days):
                alerts.append(RiskCheckResult(
                    code=code, alert_type="持仓超期",
                    message=f"买入日={buy_date} 当前={current_date}",
                    action="到期卖出",
                ))

            # 集中度
            if check_single_position_weight(price * shares, current_nav, self.max_single_position_weight):
                alerts.append(RiskCheckResult(
                    code=code, alert_type="集中度超限",
                    message=f"权重={price * shares / current_nav:.2%}",
                    action="减仓至上限",
                ))

        if alerts:
            logger.warning("风控检查发现 %d 个预警", len(alerts))
        else:
            logger.info("风控检查通过")
        return alerts
