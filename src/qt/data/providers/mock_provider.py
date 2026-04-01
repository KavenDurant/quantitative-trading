from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qt.data.ingest.universe_builder import RawInstrument
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot


@dataclass(slots=True)
class MockDataset:
    instruments: list[RawInstrument]
    fundamentals: list[FundamentalSnapshot]
    latest_prices: dict[str, float]


def load_mock_dataset(as_of_date: str) -> MockDataset:
    instruments = [
        RawInstrument(code="600519", name="贵州茅台", list_date="2001-08-27", exchange="SH", board="main"),
        RawInstrument(code="600036", name="招商银行", list_date="2002-04-09", exchange="SH", board="main"),
        RawInstrument(code="000651", name="格力电器", list_date="1996-11-18", exchange="SZ", board="main"),
        RawInstrument(code="002415", name="海康威视", list_date="2010-05-28", exchange="SZ", board="main"),
        RawInstrument(code="601318", name="中国平安", list_date="2007-03-01", exchange="SH", board="main"),
        RawInstrument(code="300750", name="宁德时代", list_date="2018-06-11", exchange="SZ", board="gem"),
    ]
    fundamentals = [
        FundamentalSnapshot(as_of_date, "600519", 0.31, 0.92, 0.88, 24.0, 8.6, 12.1, 0.18, 0.15),
        FundamentalSnapshot(as_of_date, "600036", 0.16, 0.41, 0.72, 6.1, 1.1, 2.7, 0.08, 0.07),
        FundamentalSnapshot(as_of_date, "000651", 0.22, 0.28, 0.64, 8.9, 1.9, 1.3, 0.12, 0.09),
        FundamentalSnapshot(as_of_date, "002415", 0.19, 0.46, 0.71, 16.5, 3.3, 4.5, 0.14, 0.11),
        FundamentalSnapshot(as_of_date, "601318", 0.14, 0.22, 0.61, 7.4, 1.2, 0.9, 0.1, 0.06),
        FundamentalSnapshot(as_of_date, "300750", 0.24, 0.24, 0.58, 30.0, 5.1, 5.2, 0.2, 0.22),
    ]
    latest_prices = {
        "600519": 1680.0,
        "600036": 12.8,
        "000651": 14.6,
        "002415": 9.5,
        "601318": 18.2,
        "300750": 210.0,
    }
    return MockDataset(instruments=instruments, fundamentals=fundamentals, latest_prices=latest_prices)
