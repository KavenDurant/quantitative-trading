from __future__ import annotations

import pandas as pd

from qt.data.providers.mock_provider import load_mock_dataset
from qt.data.storage.repository import DailyPrice


def load_mock_prices(start_date: str, end_date: str) -> list[DailyPrice]:
    dataset = load_mock_dataset(end_date)
    dates = pd.date_range(start=start_date, end=end_date, freq="B")
    base_prices = dataset.latest_prices
    rows: list[DailyPrice] = []

    for idx, trade_date in enumerate(dates):
        drift = 1 + idx * 0.001
        for code, price in base_prices.items():
            rows.append(
                DailyPrice(
                    trade_date=trade_date.strftime("%Y-%m-%d"),
                    code=code,
                    close=round(price * drift, 2),
                )
            )
    return rows
