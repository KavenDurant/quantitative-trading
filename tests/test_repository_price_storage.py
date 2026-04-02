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


def test_repository_load_market_proxy_prices_returns_chronological_series(tmp_path: Path):
    db_path = tmp_path / "test.db"
    client = SQLiteClient(db_path)
    client.init_db()

    with client.connect() as connection:
        repository = Repository(connection)
        repository.upsert_prices(
            [
                DailyPrice(trade_date="2025-01-01", code="AAA", close=10.0),
                DailyPrice(trade_date="2025-01-01", code="BBB", close=30.0),
                DailyPrice(trade_date="2025-01-02", code="AAA", close=20.0),
                DailyPrice(trade_date="2025-01-02", code="BBB", close=40.0),
                DailyPrice(trade_date="2025-01-03", code="AAA", close=30.0),
                DailyPrice(trade_date="2025-01-03", code="BBB", close=50.0),
            ]
        )
        proxy = repository.load_market_proxy_prices("2025-01-03", window=3)

    assert proxy == [20.0, 30.0, 40.0]


def test_repository_load_backtest_nav_by_run_returns_ordered_rows(tmp_path: Path):
    db_path = tmp_path / "test.db"
    client = SQLiteClient(db_path)
    client.init_db()

    with client.connect() as connection:
        repository = Repository(connection)
        repository.save_backtest_run("run-a", "mock", "2025-01-01", "2025-01-31")
        repository.save_backtest_nav("run-a", "2025-01-02", cash=9000.0, nav=10050.0)
        repository.save_backtest_nav("run-a", "2025-01-01", cash=10000.0, nav=10000.0)

        nav = repository.load_backtest_nav_by_run("run-a")
        run_ids = repository.load_recent_backtest_run_ids(limit=2)

    assert list(nav["trade_date"]) == ["2025-01-01", "2025-01-02"]
    assert run_ids == ["run-a"]
