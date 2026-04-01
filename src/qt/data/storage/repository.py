from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from sqlite3 import Connection

import pandas as pd


@dataclass(slots=True)
class Instrument:
    code: str
    name: str
    exchange: str
    board: str
    list_date: str
    is_st: int
    is_suspended: int


@dataclass(slots=True)
class FundamentalSnapshot:
    as_of_date: str
    code: str
    roe: float
    gross_margin: float
    operating_cashflow_ratio: float
    pe_ttm: float
    pb: float
    ps_ttm: float
    net_profit_yoy: float
    revenue_yoy: float


@dataclass(slots=True)
class DailyPrice:
    trade_date: str
    code: str
    close: float
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: float = 0.0
    amount: float = 0.0
    turnover: float = 0.0


@dataclass(slots=True)
class ValuationSnapshot:
    trade_date: str
    code: str
    pe_ttm: float = 0.0
    pb: float = 0.0
    ps_ttm: float = 0.0
    pcf_ttm: float = 0.0
    dividend_yield: float = 0.0
    total_mv: float = 0.0
    circ_mv: float = 0.0


@dataclass(slots=True)
class AnalystExpectation:
    as_of_date: str
    code: str
    eps_current_year: float = 0.0
    eps_next_year: float = 0.0
    eps_revision_pct: float = 0.0
    target_price: float = 0.0
    rating_score: float = 0.0
    coverage_count: int = 0


@dataclass(slots=True)
class EarningsSurprise:
    announce_date: str
    code: str
    report_period: str
    actual_profit: float = 0.0
    expected_profit: float = 0.0
    surprise_pct: float = 0.0
    surprise_type: str = ""


class Repository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def seed_portfolio(self, cash: float) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO portfolio_state (portfolio_id, cash, updated_at) VALUES (1, ?, ?)",
            (cash, datetime.utcnow().isoformat()),
        )
        self.connection.commit()

    def upsert_instruments(self, instruments: list[Instrument]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO instrument_master
            (code, name, exchange, board, list_date, is_st, is_suspended)
            VALUES (:code, :name, :exchange, :board, :list_date, :is_st, :is_suspended)
            """,
            [asdict(item) for item in instruments],
        )
        self.connection.commit()

    def upsert_fundamentals(self, snapshots: list[FundamentalSnapshot]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO fundamentals
            (as_of_date, code, roe, gross_margin, operating_cashflow_ratio, pe_ttm, pb, ps_ttm, net_profit_yoy, revenue_yoy)
            VALUES (:as_of_date, :code, :roe, :gross_margin, :operating_cashflow_ratio, :pe_ttm, :pb, :ps_ttm, :net_profit_yoy, :revenue_yoy)
            """,
            [asdict(item) for item in snapshots],
        )
        self.connection.commit()

    def upsert_prices(self, prices: list[DailyPrice]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO prices_daily
            (trade_date, code, open, high, low, close, volume, amount, turnover)
            VALUES (:trade_date, :code, :open, :high, :low, :close, :volume, :amount, :turnover)
            """,
            [asdict(item) for item in prices],
        )
        self.connection.commit()

    def load_latest_fundamentals(self, as_of_date: str) -> pd.DataFrame:
        query = """
        SELECT *
        FROM fundamentals
        WHERE as_of_date = (
            SELECT MAX(as_of_date) FROM fundamentals
        )
        """
        return pd.read_sql_query(query, self.connection)

    def load_prices_for_date(self, trade_date: str) -> pd.DataFrame:
        return pd.read_sql_query(
            """SELECT trade_date, code, close FROM prices_daily
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM prices_daily WHERE trade_date <= ?
               )""",
            self.connection,
            params=(trade_date,),
        )

    def save_backtest_run(self, run_id: str, provider: str, start_date: str, end_date: str) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO backtest_runs (run_id, provider, start_date, end_date, created_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, provider, start_date, end_date, datetime.utcnow().isoformat()),
        )
        self.connection.commit()

    def save_backtest_nav(self, run_id: str, trade_date: str, cash: float, nav: float) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO backtest_nav (run_id, trade_date, cash, nav) VALUES (?, ?, ?, ?)",
            (run_id, trade_date, cash, nav),
        )
        self.connection.commit()

    def save_backtest_positions(self, run_id: str, trade_date: str, positions: dict[str, int], prices: dict[str, float]) -> None:
        self.connection.executemany(
            "INSERT OR REPLACE INTO backtest_positions (run_id, trade_date, code, shares, price) VALUES (?, ?, ?, ?, ?)",
            [(run_id, trade_date, code, shares, prices.get(code, 0.0)) for code, shares in positions.items()],
        )
        self.connection.commit()

    def save_backtest_trades(self, run_id: str, trade_date: str, trades: list[tuple[str, str, int, float, float]]) -> None:
        self.connection.executemany(
            "INSERT OR REPLACE INTO backtest_trades (run_id, trade_date, code, side, shares, price, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(run_id, trade_date, code, side, shares, price, amount) for code, side, shares, price, amount in trades],
        )
        self.connection.commit()

    def load_latest_backtest_nav(self) -> pd.DataFrame:
        query = """
        SELECT n.*
        FROM backtest_nav n
        JOIN (
            SELECT run_id
            FROM backtest_runs
            ORDER BY created_at DESC
            LIMIT 1
        ) r ON n.run_id = r.run_id
        ORDER BY n.trade_date
        """
        return pd.read_sql_query(query, self.connection)

    def load_latest_backtest_positions(self) -> pd.DataFrame:
        query = """
        SELECT p.*
        FROM backtest_positions p
        JOIN (
            SELECT run_id, MAX(trade_date) AS trade_date
            FROM backtest_positions
            GROUP BY run_id
            ORDER BY trade_date DESC
            LIMIT 1
        ) latest ON p.run_id = latest.run_id AND p.trade_date = latest.trade_date
        ORDER BY p.code
        """
        return pd.read_sql_query(query, self.connection)

    def load_latest_backtest_trades(self) -> pd.DataFrame:
        query = """
        SELECT t.*
        FROM backtest_trades t
        JOIN (
            SELECT run_id, MAX(trade_date) AS trade_date
            FROM backtest_trades
            GROUP BY run_id
            ORDER BY trade_date DESC
            LIMIT 1
        ) latest ON t.run_id = latest.run_id AND t.trade_date = latest.trade_date
        ORDER BY t.code
        """
        return pd.read_sql_query(query, self.connection)

    # --- Valuation ---
    def upsert_valuations(self, snapshots: list[ValuationSnapshot]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO valuation
            (trade_date, code, pe_ttm, pb, ps_ttm, pcf_ttm, dividend_yield, total_mv, circ_mv)
            VALUES (:trade_date, :code, :pe_ttm, :pb, :ps_ttm, :pcf_ttm, :dividend_yield, :total_mv, :circ_mv)
            """,
            [asdict(item) for item in snapshots],
        )
        self.connection.commit()

    def load_latest_valuation(self, as_of_date: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM valuation WHERE trade_date = (SELECT MAX(trade_date) FROM valuation WHERE trade_date <= ?)",
            self.connection,
            params=(as_of_date,),
        )

    # --- Analyst Expectation ---
    def upsert_analyst_expectations(self, rows: list[AnalystExpectation]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO analyst_expectation
            (as_of_date, code, eps_current_year, eps_next_year, eps_revision_pct, target_price, rating_score, coverage_count)
            VALUES (:as_of_date, :code, :eps_current_year, :eps_next_year, :eps_revision_pct, :target_price, :rating_score, :coverage_count)
            """,
            [asdict(item) for item in rows],
        )
        self.connection.commit()

    def load_latest_analyst_expectations(self, as_of_date: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM analyst_expectation WHERE as_of_date = (SELECT MAX(as_of_date) FROM analyst_expectation WHERE as_of_date <= ?)",
            self.connection,
            params=(as_of_date,),
        )

    # --- Earnings Surprise ---
    def upsert_earnings_surprises(self, rows: list[EarningsSurprise]) -> None:
        self.connection.executemany(
            """
            INSERT OR REPLACE INTO earnings_surprise
            (announce_date, code, report_period, actual_profit, expected_profit, surprise_pct, surprise_type)
            VALUES (:announce_date, :code, :report_period, :actual_profit, :expected_profit, :surprise_pct, :surprise_type)
            """,
            [asdict(item) for item in rows],
        )
        self.connection.commit()

    def load_earnings_surprises(self, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM earnings_surprise WHERE announce_date BETWEEN ? AND ? ORDER BY announce_date DESC",
            self.connection,
            params=(start_date, end_date),
        )
