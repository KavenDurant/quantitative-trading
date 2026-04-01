"""回填历史数据 — 使用真实数据源填充数据库

支持 baostock（推荐）和 akshare 两种数据源。
用法: PYTHONPATH=src python -m qt.pipelines.backfill_data
"""
from __future__ import annotations

from pathlib import Path

from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import filter_universe
from qt.data.providers.baostock_provider import BaostockProvider
from qt.data.providers.provider_factory import get_provider
from qt.data.quality_check import DataQualityChecker
from qt.data.storage.repository import DailyPrice, Instrument, Repository
from qt.data.storage.sqlite_client import SQLiteClient

logger = get_logger(__name__)


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
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

        # 6. 数据质量检查
        checker = DataQualityChecker(connection)
        checker.run_all()

    logger.info(
        "回填完成 provider=%s instruments=%d fundamentals=%d prices=%d",
        provider_name, len(filtered), len(eligible_fundamentals), len(prices),
    )


if __name__ == "__main__":
    main()
