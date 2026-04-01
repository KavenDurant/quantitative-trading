from pathlib import Path

from qt.data.storage.repository import DailyPrice, Repository
from qt.data.storage.sqlite_client import SQLiteClient


def test_repository_can_store_and_read_prices(tmp_path: Path):
    db_path = tmp_path / "test.db"
    client = SQLiteClient(db_path)
    client.init_db()

    with client.connect() as connection:
        repository = Repository(connection)
        repository.upsert_prices([DailyPrice(trade_date="2025-01-31", code="600036", close=35.5)])
        prices = repository.load_prices_for_date("2025-01-31")

    assert len(prices) == 1
    assert prices.iloc[0]["code"] == "600036"
