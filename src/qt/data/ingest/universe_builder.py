from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class RawInstrument:
    code: str
    name: str
    list_date: str
    exchange: str
    board: str
    is_st: bool = False
    is_suspended: bool = False


def is_main_board(code: str, exchange: str) -> bool:
    if exchange == "SH":
        return code.startswith("600") or code.startswith("601") or code.startswith("603") or code.startswith("605")
    if exchange == "SZ":
        return code.startswith("000") or code.startswith("001") or code.startswith("002")
    return False


def listing_days(list_date: str, as_of: str) -> int:
    try:
        ld = datetime.strptime(list_date, "%Y-%m-%d").date()
        ao = datetime.strptime(as_of, "%Y-%m-%d").date()
        return (ao - ld).days
    except (ValueError, TypeError):
        return 0


def filter_universe(
    instruments: list[RawInstrument],
    *,
    min_listing_days: int = 365,
    as_of_date: str | None = None,
) -> list[RawInstrument]:
    ref_date = as_of_date or date.today().isoformat()
    return [
        item
        for item in instruments
        if is_main_board(item.code, item.exchange)
        and not item.is_st
        and not item.is_suspended
        and listing_days(item.list_date, ref_date) >= min_listing_days
    ]


def filter_by_price(
    codes_with_prices: dict[str, float],
    min_price: float = 5.0,
    max_price: float = 50.0,
) -> set[str]:
    return {code for code, price in codes_with_prices.items() if min_price <= price <= max_price}


def filter_by_liquidity(
    codes_with_amount: dict[str, float],
    min_daily_turnover: float = 30_000_000,
) -> set[str]:
    return {code for code, amt in codes_with_amount.items() if amt >= min_daily_turnover}
