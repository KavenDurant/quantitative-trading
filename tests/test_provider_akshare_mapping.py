from pathlib import Path

from qt.common.config import load_app_config
from qt.data.ingest.universe_builder import RawInstrument
from qt.data.providers.baostock_provider import BaostockProvider
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot


def test_load_app_config_reads_provider_and_backtest_window():
    project_root = Path("/Volumes/WD-1TB/WebstormProjects/quantitative-trading")
    config = load_app_config(project_root)

    assert config.data_provider == "baostock"
    assert config.backtest_start == "2025-01-01"
    assert config.backtest_end == "2025-12-31"


def test_baostock_historical_dataset_keeps_all_main_board_codes(monkeypatch):
    provider = BaostockProvider()
    codes = [f"600{i:03d}" for i in range(60)]

    monkeypatch.setattr("qt.data.providers.baostock_provider._login", lambda: None)
    monkeypatch.setattr("qt.data.providers.baostock_provider._logout", lambda: None)
    monkeypatch.setattr(
        provider,
        "_load_instruments",
        lambda _date: [
            RawInstrument(
                code=code,
                name=f"股票{idx}",
                list_date="2000-01-01",
                exchange="SH",
                board="main",
                is_st=False,
                is_suspended=False,
            )
            for idx, code in enumerate(codes)
        ],
    )
    monkeypatch.setattr(
        provider,
        "_load_fundamentals",
        lambda in_codes, as_of_date: [
            FundamentalSnapshot(as_of_date, code, 1, 1, 1, 1, 1, 1, 1, 1)
            for code in in_codes
        ],
    )
    monkeypatch.setattr(
        provider,
        "_load_prices",
        lambda in_codes, start_date, end_date: [
            DailyPrice(trade_date=end_date, code=code, close=10.0)
            for code in in_codes
        ],
    )

    dataset = provider.load_historical_dataset("2025-01-01", "2025-12-31")

    assert len(dataset.instruments) == 60
    assert len(dataset.fundamentals) == 60
    assert len(dataset.latest_prices) == 60
