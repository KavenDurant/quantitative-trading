from __future__ import annotations

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS instrument_master (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        exchange TEXT NOT NULL,
        board TEXT NOT NULL,
        list_date TEXT NOT NULL,
        is_st INTEGER NOT NULL,
        is_suspended INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamentals (
        as_of_date TEXT NOT NULL,
        code TEXT NOT NULL,
        roe REAL NOT NULL,
        gross_margin REAL NOT NULL,
        operating_cashflow_ratio REAL NOT NULL,
        pe_ttm REAL NOT NULL,
        pb REAL NOT NULL,
        ps_ttm REAL NOT NULL,
        net_profit_yoy REAL NOT NULL,
        revenue_yoy REAL NOT NULL,
        PRIMARY KEY (as_of_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS prices_daily (
        trade_date TEXT NOT NULL,
        code TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL NOT NULL,
        volume REAL,
        amount REAL,
        turnover REAL,
        PRIMARY KEY (trade_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS composite_scores (
        as_of_date TEXT NOT NULL,
        code TEXT NOT NULL,
        quality_score REAL NOT NULL,
        value_score REAL NOT NULL,
        expectation_score REAL NOT NULL,
        composite_score REAL NOT NULL,
        rank_value INTEGER NOT NULL,
        PRIMARY KEY (as_of_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rebalance_signals (
        rebalance_id TEXT NOT NULL,
        as_of_date TEXT NOT NULL,
        code TEXT NOT NULL,
        action TEXT NOT NULL,
        target_weight REAL NOT NULL,
        target_shares INTEGER NOT NULL,
        price REAL NOT NULL,
        PRIMARY KEY (rebalance_id, code, action)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        rebalance_id TEXT NOT NULL,
        code TEXT NOT NULL,
        side TEXT NOT NULL,
        shares INTEGER NOT NULL,
        price REAL NOT NULL,
        amount REAL NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        code TEXT PRIMARY KEY,
        shares INTEGER NOT NULL,
        avg_cost REAL NOT NULL,
        last_price REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_state (
        portfolio_id INTEGER PRIMARY KEY CHECK (portfolio_id = 1),
        cash REAL NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_runs (
        run_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_nav (
        run_id TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        cash REAL NOT NULL,
        nav REAL NOT NULL,
        PRIMARY KEY (run_id, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_positions (
        run_id TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        code TEXT NOT NULL,
        shares INTEGER NOT NULL,
        price REAL NOT NULL,
        PRIMARY KEY (run_id, trade_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_trades (
        run_id TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        code TEXT NOT NULL,
        side TEXT NOT NULL,
        shares INTEGER NOT NULL,
        price REAL NOT NULL,
        amount REAL NOT NULL,
        PRIMARY KEY (run_id, trade_date, code, side)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS valuation (
        trade_date TEXT NOT NULL,
        code TEXT NOT NULL,
        pe_ttm REAL,
        pb REAL,
        ps_ttm REAL,
        pcf_ttm REAL,
        dividend_yield REAL,
        total_mv REAL,
        circ_mv REAL,
        PRIMARY KEY (trade_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS analyst_expectation (
        as_of_date TEXT NOT NULL,
        code TEXT NOT NULL,
        eps_current_year REAL,
        eps_next_year REAL,
        eps_revision_pct REAL,
        target_price REAL,
        rating_score REAL,
        coverage_count INTEGER,
        PRIMARY KEY (as_of_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS earnings_surprise (
        announce_date TEXT NOT NULL,
        code TEXT NOT NULL,
        report_period TEXT NOT NULL,
        actual_profit REAL,
        expected_profit REAL,
        surprise_pct REAL,
        surprise_type TEXT,
        PRIMARY KEY (announce_date, code, report_period)
    )
    """,
]
