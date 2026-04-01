from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


def month_end(day: date) -> date:
    next_month = day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def month_end_dates(start_date: str, end_date: str) -> list[str]:
    dates = pd.date_range(start=start_date, end=end_date, freq="ME")
    return [item.strftime("%Y-%m-%d") for item in dates]
