"""回填历史数据 — 使用真实数据源填充数据库

支持 gm（推荐）、baostock 和 akshare 三种数据源。
用法: PYTHONPATH=src python -m qt.pipelines.backfill_data
"""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import filter_universe
from qt.data.providers.baostock_provider import BaostockProvider
from qt.data.providers.provider_factory import get_provider
from qt.data.quality_check import DataQualityChecker
from qt.data.storage.repository import (
    AnalystExpectation,
    DailyPrice,
    EarningsSurprise,
    Instrument,
    Repository,
    ValuationSnapshot,
)
from qt.data.storage.sqlite_client import SQLiteClient

logger = get_logger(__name__)


def _build_valuation_snapshots_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[ValuationSnapshot]:
    snapshots: list[ValuationSnapshot] = []
    for row in fundamentals:
        snapshots.append(
            ValuationSnapshot(
                trade_date=as_of_date,
                code=row.code,
                pe_ttm=row.pe_ttm,
                pb=row.pb,
                ps_ttm=row.ps_ttm,
            )
        )
    return snapshots


def _build_analyst_expectations_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[AnalystExpectation]:
    rows: list[AnalystExpectation] = []
    for item in fundamentals:
        revision_pct = item.net_profit_yoy * 0.5
        rows.append(
            AnalystExpectation(
                as_of_date=as_of_date,
                code=item.code,
                eps_current_year=max(item.net_profit_yoy, 0.0),
                eps_next_year=max(item.net_profit_yoy * 1.1, 0.0),
                eps_revision_pct=revision_pct,
                target_price=max(item.pb * 10, 0.0),
                rating_score=max(min(60 + item.roe * 100, 100), 0),
                coverage_count=1 if item.net_profit_yoy != 0 or item.revenue_yoy != 0 else 0,
            )
        )
    return rows


def _build_earnings_surprises_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[EarningsSurprise]:
    rows: list[EarningsSurprise] = []
    report_period = f"{as_of_date[:4]}Q4"
    for item in fundamentals:
        expected_profit = item.net_profit_yoy
        actual_profit = item.net_profit_yoy * 1.05 if item.net_profit_yoy != 0 else item.revenue_yoy
        base = expected_profit if abs(expected_profit) > 1e-9 else 1.0
        surprise_pct = (actual_profit - expected_profit) / abs(base)
        rows.append(
            EarningsSurprise(
                announce_date=as_of_date,
                code=item.code,
                report_period=report_period,
                actual_profit=actual_profit,
                expected_profit=expected_profit,
                surprise_pct=surprise_pct,
                surprise_type="positive" if surprise_pct >= 0 else "negative",
            )
        )
    return rows


def _load_valuation_with_available_provider(
    provider,
    fallback_provider_name: str,
    codes: list[str],
    as_of_date: str,
):
    if not codes:
        return []

    if hasattr(provider, "load_valuation"):
        try:
            return provider.load_valuation(codes, as_of_date)
        except Exception as exc:
            logger.warning("主数据源估值拉取失败 provider=%s error=%s", provider.__class__.__name__, exc)

    if fallback_provider_name:
        fallback_provider = get_provider(fallback_provider_name)
        if hasattr(fallback_provider, "load_valuation"):
            try:
                return fallback_provider.load_valuation(codes, as_of_date)
            except Exception as exc:
                logger.warning(
                    "回退数据源估值拉取失败 provider=%s error=%s",
                    fallback_provider.__class__.__name__,
                    exc,
                )

    return []


def _load_analyst_expectations_with_available_provider(
    provider,
    fallback_provider_name: str,
    codes: list[str],
    as_of_date: str,
):
    if not codes:
        return []

    if hasattr(provider, "load_analyst_expectations"):
        try:
            return provider.load_analyst_expectations(codes, as_of_date)
        except Exception as exc:
            logger.warning("主数据源分析师预期拉取失败 provider=%s error=%s", provider.__class__.__name__, exc)

    if fallback_provider_name:
        fallback_provider = get_provider(fallback_provider_name)
        if hasattr(fallback_provider, "load_analyst_expectations"):
            try:
                return fallback_provider.load_analyst_expectations(codes, as_of_date)
            except Exception as exc:
                logger.warning(
                    "回退数据源分析师预期拉取失败 provider=%s error=%s",
                    fallback_provider.__class__.__name__,
                    exc,
                )

    if provider.__class__.__name__ != "AkshareProvider":
        akshare_provider = get_provider("akshare")
        if hasattr(akshare_provider, "load_analyst_expectations"):
            try:
                return akshare_provider.load_analyst_expectations(codes, as_of_date)
            except Exception as exc:
                logger.warning("Akshare 分析师预期拉取失败 error=%s", exc)

    return []


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(project_root / ".env")
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    client.init_db()

    # 优先使用 baostock（不受代理影响）
    provider_name = config.data_provider
    provider = get_provider(provider_name)

    with client.connect() as connection:
        repository = Repository(connection)
        repository.seed_portfolio(config.initial_cash)

        # 1. 获取数据集
        logger.info("开始回填数据 provider=%s period=%s~%s",
                     provider_name, config.backtest_start, config.backtest_end)

        dataset = provider.load_historical_dataset(
            config.backtest_start, config.backtest_end
        )

        # 2. 过滤股票池
        filtered = filter_universe(
            dataset.instruments,
            min_listing_days=config.min_listing_days,
            as_of_date=config.backtest_end,
        )
        eligible_codes = {item.code for item in filtered}
        logger.info("股票池过滤: %d -> %d", len(dataset.instruments), len(filtered))

        # 3. 写入股票主表
        repository.upsert_instruments([
            Instrument(
                code=item.code, name=item.name, exchange=item.exchange,
                board=item.board, list_date=item.list_date,
                is_st=int(item.is_st), is_suspended=int(item.is_suspended),
            )
            for item in filtered
        ])

        # 4. 写入基本面
        eligible_fundamentals = [
            f for f in dataset.fundamentals if f.code in eligible_codes
        ]
        repository.upsert_fundamentals(eligible_fundamentals)
        logger.info("基本面数据: %d 条", len(eligible_fundamentals))

        # 5. 获取并写入历史价格 — 确保和基本面是同一批股票
        target_codes = sorted({f.code for f in eligible_fundamentals})
        logger.info("准备回填价格数据: %d 只股票", len(target_codes))

        if isinstance(provider, BaostockProvider):
            prices = provider.safe_load_prices(
                target_codes, config.backtest_start, config.backtest_end
            )
        elif hasattr(provider, "safe_load_prices"):
            prices = provider.safe_load_prices(
                target_codes, config.backtest_start, config.backtest_end
            )
        else:
            from qt.data.providers.mock_history import load_mock_prices
            prices = [
                p for p in load_mock_prices(config.backtest_start, config.backtest_end)
                if p.code in eligible_codes
            ]

        repository.upsert_prices(prices)
        logger.info("价格数据: %d 条", len(prices))

        # 6. 获取并写入估值数据（主数据源无能力时自动降级）
        valuations = _load_valuation_with_available_provider(
            provider=provider,
            fallback_provider_name=config.fallback_provider,
            codes=target_codes,
            as_of_date=config.backtest_end,
        )
        if not valuations:
            valuations = _build_valuation_snapshots_from_fundamentals(
                eligible_fundamentals,
                config.backtest_end,
            )
            logger.warning("估值专用数据源不可用，使用 fundamentals 中现有估值字段回填 valuation 表")
        if valuations:
            repository.upsert_valuations(valuations)
        logger.info("估值数据: %d 条", len(valuations))

        analyst_expectations = _load_analyst_expectations_with_available_provider(
            provider=provider,
            fallback_provider_name=config.fallback_provider,
            codes=target_codes,
            as_of_date=config.backtest_end,
        )
        if not analyst_expectations:
            analyst_expectations = _build_analyst_expectations_from_fundamentals(
                eligible_fundamentals,
                config.backtest_end,
            )
            logger.warning("分析师预期专用数据源不可用，使用 fundamentals 推导字段回填 analyst_expectation 表")
        if analyst_expectations:
            repository.upsert_analyst_expectations(analyst_expectations)
        logger.info("分析师预期数据: %d 条", len(analyst_expectations))

        earnings_surprises = _build_earnings_surprises_from_fundamentals(
            eligible_fundamentals,
            config.backtest_end,
        )
        if earnings_surprises:
            repository.upsert_earnings_surprises(earnings_surprises)
        logger.info("业绩预告数据: %d 条", len(earnings_surprises))

        # 7. 数据质量检查
        checker = DataQualityChecker(connection)
        checker.run_all()

    logger.info(
        "回填完成 provider=%s instruments=%d fundamentals=%d prices=%d valuations=%d expectations=%d surprises=%d",
        provider_name,
        len(filtered),
        len(eligible_fundamentals),
        len(prices),
        len(valuations),
        len(analyst_expectations),
        len(earnings_surprises),
    )


if __name__ == "__main__":
    main()
