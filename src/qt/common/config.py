from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class AppConfig:
    initial_cash: float
    max_positions: int
    lot_size: int
    min_listing_days: int
    min_price: float
    max_price: float
    min_daily_turnover: float
    cash_buffer_pct: float
    commission_rate: float
    slippage_rate: float
    stop_loss_pct: float
    take_profit_1_pct: float
    take_profit_2_pct: float
    holding_period_days: int
    monthly_portfolio_stop_loss_pct: float
    max_single_position_weight: float
    market_timing_enabled: bool
    market_timing_short_window: int
    market_timing_long_window: int
    rebalance_frequency: str
    rebalance_day: str
    backtest_start: str
    backtest_end: str
    benchmark: str
    factor_weights: dict[str, float]
    factor_columns: dict[str, list[str]]
    db_path: Path
    data_provider: str
    fallback_provider: str
    data_as_of: str
    exclude_prefixes: list[str]


@dataclass(slots=True)
class ScheduleConfig:
    daily_checks: str
    monthly_rebalance: str
    monitor_interval_minutes: int
    close_review_time: str


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_app_config(project_root: Path) -> AppConfig:
    strategy_cfg = _read_yaml(project_root / "config" / "strategy.yaml").get("strategy", {})
    data_cfg = _read_yaml(project_root / "config" / "data_sources.yaml").get("data_sources", {})
    db_path = project_root / ".data" / "quant_trading.db"
    return AppConfig(
        initial_cash=float(strategy_cfg.get("initial_cash", 10000)),
        max_positions=int(strategy_cfg.get("max_positions", 5)),
        lot_size=int(strategy_cfg.get("lot_size", 100)),
        min_listing_days=int(strategy_cfg.get("min_listing_days", 365)),
        min_price=float(strategy_cfg.get("min_price", 5.0)),
        max_price=float(strategy_cfg.get("max_price", 50.0)),
        min_daily_turnover=float(strategy_cfg.get("min_daily_turnover", 30000000)),
        cash_buffer_pct=float(strategy_cfg.get("cash_buffer_pct", 0.02)),
        commission_rate=float(strategy_cfg.get("commission_rate", 0.0003)),
        slippage_rate=float(strategy_cfg.get("slippage_rate", 0.0005)),
        stop_loss_pct=float(strategy_cfg.get("stop_loss_pct", -0.08)),
        take_profit_1_pct=float(strategy_cfg.get("take_profit_1_pct", 0.15)),
        take_profit_2_pct=float(strategy_cfg.get("take_profit_2_pct", 0.25)),
        holding_period_days=int(strategy_cfg.get("holding_period_days", 120)),
        monthly_portfolio_stop_loss_pct=float(strategy_cfg.get("monthly_portfolio_stop_loss_pct", -0.12)),
        max_single_position_weight=float(strategy_cfg.get("max_single_position_weight", 0.2)),
        market_timing_enabled=bool(strategy_cfg.get("market_timing_enabled", False)),
        market_timing_short_window=int(strategy_cfg.get("market_timing_short_window", 20)),
        market_timing_long_window=int(strategy_cfg.get("market_timing_long_window", 60)),
        rebalance_frequency=str(strategy_cfg.get("rebalance_frequency", "monthly")),
        rebalance_day=str(strategy_cfg.get("rebalance_day", "month_end")),
        backtest_start=str(strategy_cfg.get("backtest_start", "2025-01-01")),
        backtest_end=str(strategy_cfg.get("backtest_end", "2025-12-31")),
        benchmark=str(strategy_cfg.get("benchmark", "000300.SH")),
        factor_weights=dict(strategy_cfg.get("factor_weights", {})),
        factor_columns=dict(strategy_cfg.get("factor_columns", {})),
        db_path=db_path,
        data_provider=str(data_cfg.get("primary", "mock")),
        fallback_provider=str(data_cfg.get("fallback", "mock")),
        data_as_of=str(strategy_cfg.get("data_as_of", "2025-12-31")),
        exclude_prefixes=list(data_cfg.get("universe", {}).get("exclude_prefixes", [])),
    )


def load_schedule_config(project_root: Path) -> ScheduleConfig:
    schedule_cfg = _read_yaml(project_root / "config" / "schedule.yaml").get("schedule", {})
    return ScheduleConfig(
        daily_checks=str(schedule_cfg.get("daily_checks", "0 18 * * 1-5")),
        monthly_rebalance=str(schedule_cfg.get("monthly_rebalance", "31 9 1 * *")),
        monitor_interval_minutes=int(schedule_cfg.get("monitor_interval_minutes", 5)),
        close_review_time=str(schedule_cfg.get("close_review_time", "15:10")),
    )
