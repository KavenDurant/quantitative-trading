"""Refresh fundamentals data with corrected field mapping.
Usage: PYTHONPATH=src python -u scripts/refresh_fundamentals.py
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import baostock as bs

from qt.common.config import load_app_config
from qt.data.providers.baostock_provider import (
    BaostockProvider,
    _query_to_list as bs_query,
    _to_bs_code,
)
from qt.data.storage.repository import FundamentalSnapshot, Repository
from qt.data.storage.sqlite_client import SQLiteClient


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    config = load_app_config(project_root)

    # Get all codes from DB
    conn = sqlite3.connect(str(config.db_path))
    codes = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT code FROM instrument_master ORDER BY code"
        ).fetchall()
    ]
    print(f"Total codes in DB: {len(codes)}")
    conn.close()

    as_of_date = "2026-03-31"
    year, quarter = 2025, 3
    print(f"Querying fundamentals for year={year} Q{quarter}")

    bs.login()

    results: list[FundamentalSnapshot] = []
    total = len(codes)
    safe = BaostockProvider._safe_float

    for idx, code in enumerate(codes, start=1):
        if idx % 500 == 0 or idx == total:
            print(f"Progress: {idx}/{total} ({idx / total * 100:.1f}%)", flush=True)

        bs_code = _to_bs_code(code)

        roe = 0.0
        gross_margin = 0.0
        net_profit_yoy = 0.0
        revenue_yoy = 0.0
        cashflow_ratio = 0.0

        try:
            rs_profit = bs.query_profit_data(
                code=bs_code, year=year, quarter=quarter
            )
            profit_data = bs_query(rs_profit)
            if profit_data:
                row = profit_data[-1]
                roe = safe(row[3])  # roeAvg
                gross_margin = safe(row[5])  # gpMargin
        except Exception:
            pass

        try:
            rs_growth = bs.query_growth_data(
                code=bs_code, year=year, quarter=quarter
            )
            growth_data = bs_query(rs_growth)
            if growth_data:
                row = growth_data[-1]
                net_profit_yoy = safe(row[5])  # YOYNI
                revenue_yoy = safe(row[7])  # YOYPNI
        except Exception:
            pass

        try:
            rs_cash = bs.query_cash_flow_data(
                code=bs_code, year=year, quarter=quarter
            )
            cash_data = bs_query(rs_cash)
            if cash_data:
                row = cash_data[-1]
                cashflow_ratio = safe(row[8])  # CFOToNP
        except Exception:
            pass

        results.append(
            FundamentalSnapshot(
                as_of_date=as_of_date,
                code=code,
                roe=roe,
                gross_margin=gross_margin,
                operating_cashflow_ratio=cashflow_ratio,
                pe_ttm=0.0,
                pb=0.0,
                ps_ttm=0.0,
                net_profit_yoy=net_profit_yoy,
                revenue_yoy=revenue_yoy,
            )
        )

        time.sleep(0.05)

    bs.logout()
    print(f"Got fundamentals for {len(results)} stocks")

    # Update DB
    client = SQLiteClient(config.db_path)
    with client.connect() as connection:
        import pandas as pd

        existing = pd.read_sql_query(
            "SELECT code, pe_ttm, pb, ps_ttm FROM fundamentals WHERE as_of_date = ?",
            connection,
            params=(as_of_date,),
        )
        existing_map = dict(
            zip(existing["code"], zip(existing["pe_ttm"], existing["pb"], existing["ps_ttm"]))
        )

        for r in results:
            if r.code in existing_map:
                r.pe_ttm = existing_map[r.code][0]
                r.pb = existing_map[r.code][1]
                r.ps_ttm = existing_map[r.code][2]

        connection.execute(
            "DELETE FROM fundamentals WHERE as_of_date = ?", (as_of_date,)
        )
        connection.commit()
        print("Deleted old fundamentals")

        repo = Repository(connection)
        repo.upsert_fundamentals(results)
        connection.commit()
        print("Inserted new fundamentals")

    # Verify
    conn = sqlite3.connect(str(config.db_path))
    r = conn.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE roe > 0 AND as_of_date = ?",
        (as_of_date,),
    ).fetchone()
    print(f"Stocks with ROE > 0: {r[0]}")
    r2 = conn.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE net_profit_yoy != 0 AND as_of_date = ?",
        (as_of_date,),
    ).fetchone()
    print(f"Stocks with net_profit_yoy != 0: {r2[0]}")
    r3 = conn.execute(
        "SELECT AVG(roe) FROM fundamentals WHERE as_of_date = ?", (as_of_date,)
    ).fetchone()
    print(f"Average ROE: {r3[0]}")
    r4 = conn.execute(
        "SELECT AVG(gross_margin) FROM fundamentals WHERE as_of_date = ?",
        (as_of_date,),
    ).fetchone()
    print(f"Average gross_margin: {r4[0]}")
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
