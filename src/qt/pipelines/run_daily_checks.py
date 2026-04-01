from __future__ import annotations

from datetime import date
from pathlib import Path

from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import filter_universe
from qt.data.providers.provider_factory import get_provider
from qt.data.quality_check import DataQualityChecker
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot, Instrument, Repository
from qt.data.storage.sqlite_client import SQLiteClient

logger = get_logger(__name__)


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    client.init_db()
    provider = get_provider(config.data_provider)
    today = date.today().isoformat()

    with client.connect() as connection:
        repository = Repository(connection)

        # 1. 更新股票池
        dataset = provider.load_dataset(today)
        filtered = filter_universe(dataset.instruments, as_of_date=today)
        eligible_codes = {item.code for item in filtered}
        repository.upsert_instruments(
            [
                Instrument(
                    code=item.code, name=item.name, exchange=item.exchange,
                    board=item.board, list_date=item.list_date,
                    is_st=int(item.is_st), is_suspended=int(item.is_suspended),
                )
                for item in filtered
            ]
        )

        # 2. 更新基本面
        repository.upsert_fundamentals(
            [item for item in dataset.fundamentals if item.code in eligible_codes]
        )

        # 3. 更新日行情
        if hasattr(provider, "safe_load_prices"):
            prices = provider.safe_load_prices(sorted(eligible_codes)[:20], today, today)
        else:
            prices = [DailyPrice(trade_date=today, code=code, close=p) for code, p in dataset.latest_prices.items() if code in eligible_codes]
        repository.upsert_prices(prices)

        # 4. 数据质量检查
        checker = DataQualityChecker(connection)
        checker.run_all()

    logger.info("每日数据更新完成 date=%s instruments=%s prices=%s", today, len(filtered), len(prices))


if __name__ == "__main__":
    main()
