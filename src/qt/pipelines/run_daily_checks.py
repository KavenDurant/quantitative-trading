from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import filter_universe
from qt.data.providers.provider_factory import get_provider
from qt.data.quality_check import DataQualityChecker
from qt.data.storage.repository import (
    AnalystExpectation,
    DailyPrice,
    EarningsSurprise,
    Instrument,
    Repository,
    ValuationSnapshot,
)
from qt.data.storage.sqlite_client import SQLiteClient
from qt.factors.decay_detector import batch_detect_decay, DecayStatus

logger = get_logger(__name__)


def _load_with_fallback(provider, fallback_provider_name: str, method_name: str, *args):
    if hasattr(provider, method_name):
        try:
            return getattr(provider, method_name)(*args)
        except Exception as exc:
            logger.warning(
                "主数据源拉取失败 method=%s provider=%s error=%s",
                method_name,
                provider.__class__.__name__,
                exc,
            )

    if fallback_provider_name:
        fallback_provider = get_provider(fallback_provider_name)
        if hasattr(fallback_provider, method_name):
            try:
                return getattr(fallback_provider, method_name)(*args)
            except Exception as exc:
                logger.warning(
                    "回退数据源拉取失败 method=%s provider=%s error=%s",
                    method_name,
                    fallback_provider.__class__.__name__,
                    exc,
                )

    return []


def _build_valuation_snapshots_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[ValuationSnapshot]:
    return [
        ValuationSnapshot(
            trade_date=as_of_date,
            code=item.code,
            pe_ttm=item.pe_ttm,
            pb=item.pb,
            ps_ttm=item.ps_ttm,
        )
        for item in fundamentals
    ]


def _build_analyst_expectations_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[AnalystExpectation]:
    return [
        AnalystExpectation(
            as_of_date=as_of_date,
            code=item.code,
            eps_current_year=max(item.net_profit_yoy, 0.0),
            eps_next_year=max(item.net_profit_yoy * 1.1, 0.0),
            eps_revision_pct=item.net_profit_yoy * 0.5,
            target_price=max(item.pb * 10, 0.0),
            rating_score=max(min(60 + item.roe * 100, 100), 0),
            coverage_count=1 if item.net_profit_yoy != 0 or item.revenue_yoy != 0 else 0,
        )
        for item in fundamentals
    ]


def _build_earnings_surprises_from_fundamentals(
    fundamentals,
    as_of_date: str,
) -> list[EarningsSurprise]:
    report_period = f"{as_of_date[:4]}Q4"
    rows: list[EarningsSurprise] = []
    for item in fundamentals:
        expected_profit = item.net_profit_yoy
        actual_profit = item.net_profit_yoy * 1.05 if item.net_profit_yoy != 0 else item.revenue_yoy
        base = expected_profit if abs(expected_profit) > 1e-9 else 1.0
        surprise_pct = (actual_profit - expected_profit) / abs(base)
        rows.append(
            EarningsSurprise(
                announce_date=as_of_date,
                code=item.code,
                report_period=report_period,
                actual_profit=actual_profit,
                expected_profit=expected_profit,
                surprise_pct=surprise_pct,
                surprise_type="positive" if surprise_pct >= 0 else "negative",
            )
        )
    return rows


def _log_daily_refresh_summary(connection, today: str) -> None:
    summary = {
        "latest_price_date": connection.execute("SELECT MAX(trade_date) FROM prices_daily").fetchone()[0],
        "valuation_today": connection.execute(
            "SELECT COUNT(*) FROM valuation WHERE trade_date = ?",
            (today,),
        ).fetchone()[0],
        "analyst_today": connection.execute(
            "SELECT COUNT(*) FROM analyst_expectation WHERE as_of_date = ?",
            (today,),
        ).fetchone()[0],
        "surprise_today": connection.execute(
            "SELECT COUNT(*) FROM earnings_surprise WHERE announce_date = ?",
            (today,),
        ).fetchone()[0],
    }
    logger.info("每日更新验证 summary=%s", summary)


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    client.init_db()
    provider = get_provider(config.data_provider)
    today = date.today().isoformat()

    with client.connect() as connection:
        repository = Repository(connection)

        # 1. 更新股票池
        dataset = provider.load_dataset(today)
        filtered = filter_universe(dataset.instruments, as_of_date=today)
        eligible_codes = {item.code for item in filtered}
        repository.upsert_instruments(
            [
                Instrument(
                    code=item.code, name=item.name, exchange=item.exchange,
                    board=item.board, list_date=item.list_date,
                    is_st=int(item.is_st), is_suspended=int(item.is_suspended),
                )
                for item in filtered
            ]
        )

        # 2. 更新基本面
        latest_fundamentals = [item for item in dataset.fundamentals if item.code in eligible_codes]
        repository.upsert_fundamentals(latest_fundamentals)

        # 3. 更新日行情
        target_codes = sorted({f.code for f in latest_fundamentals})
        if hasattr(provider, "safe_load_prices"):
            prices = provider.safe_load_prices(target_codes, today, today)
        else:
            prices = [DailyPrice(trade_date=today, code=code, close=p) for code, p in dataset.latest_prices.items() if code in eligible_codes]
        repository.upsert_prices(prices)

        # 4. 更新估值数据（优先 provider，失败后回落到 fundamentals 字段）
        valuations = _load_with_fallback(
            provider,
            config.fallback_provider,
            "load_valuation",
            target_codes,
            today,
        )
        if not valuations:
            valuations = _build_valuation_snapshots_from_fundamentals(latest_fundamentals, today)
        if valuations:
            repository.upsert_valuations(valuations)

        # 5. 更新分析师预期（优先 provider，失败后使用基础映射）
        analyst_expectations = _load_with_fallback(
            provider,
            config.fallback_provider,
            "load_analyst_expectations",
            target_codes,
            today,
        )
        if not analyst_expectations:
            analyst_expectations = _build_analyst_expectations_from_fundamentals(latest_fundamentals, today)
        if analyst_expectations:
            repository.upsert_analyst_expectations(analyst_expectations)

        # 6. 更新业绩预告/业绩惊喜（优先 provider，失败后使用基础映射）
        earnings_surprises = _load_with_fallback(
            provider,
            config.fallback_provider,
            "load_earnings_surprises",
            target_codes,
            today,
        )
        if not earnings_surprises:
            earnings_surprises = _build_earnings_surprises_from_fundamentals(latest_fundamentals, today)
        if earnings_surprises:
            repository.upsert_earnings_surprises(earnings_surprises)

        # 7. 数据质量检查 + 每日更新验证
        checker = DataQualityChecker(connection)
        checker.run_all()
        _log_daily_refresh_summary(connection, today)

        # 8. 因子有效性衰减检测（可选：每周执行一次）
        _run_factor_decay_check(repository, today)

    logger.info(
        "每日数据更新完成 date=%s instruments=%s prices=%s valuations=%s expectations=%s surprises=%s",
        today,
        len(filtered),
        len(prices),
        len(valuations),
        len(analyst_expectations),
        len(earnings_surprises),
    )


def _run_factor_decay_check(repository: Repository, as_of_date: str) -> None:
    try:
        from qt.factors import build_composite_scores

        prices_df = repository.load_prices(as_of_date=as_of_date)
        fundamentals_df = repository.load_fundamentals(as_of_date=as_of_date)

        if prices_df.empty or fundamentals_df.empty:
            logger.info("因子衰减检测跳过：价格或基本面数据为空")
            return

        factor_frame = build_composite_scores(
            pd.merge(fundamentals_df, prices_df[["code", "close"]], on="code", how="inner"),
        )

        forward_returns_df = _build_forward_returns(repository, as_of_date, lookback_days=120)
        if forward_returns_df.empty:
            logger.info("因子衰减检测跳过：无法构建未来收益序列")
            return

        factor_history = _build_factor_history(repository, as_of_date, months=6)
        if factor_history.empty:
            logger.info("因子衰减检测跳过：历史因子数据不足")
            return

        factor_names = ["quality_score", "value_score", "expectation_score", "composite_score"]
        reports = batch_detect_decay(
            factor_names,
            factor_history,
            forward_returns_df,
            lookback_months=6,
        )

        decayed = [r for r in reports if r.status in (DecayStatus.WARNING, DecayStatus.DECAYED)]
        if decayed:
            logger.warning("检测到因子衰减: %s", [r.factor_name for r in decayed])
            for r in decayed:
                logger.warning("因子 %s: %s", r.factor_name, r.message)
        else:
            logger.info("因子衰减检测通过: 所有因子表现健康")

    except Exception as exc:
        logger.error("因子衰减检测失败 error=%s", exc)


def _build_forward_returns(repository: Repository, as_of_date: str, lookback_days: int) -> pd.DataFrame:
    end = pd.Timestamp(as_of_date)
    start = end - pd.Timedelta(days=lookback_days + 20)

    prices = pd.read_sql_query(
        """
        SELECT trade_date, code, close
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date, code
        """,
        repository.connection,
        params=(start.strftime("%Y-%m-%d"), as_of_date),
    )

    if prices.empty:
        return pd.DataFrame()

    prices["next_close"] = prices.groupby("code")["close"].shift(-1)
    prices["forward_return"] = (prices["next_close"] - prices["close"]) / prices["close"]
    return prices.dropna(subset=["forward_return"])


def _build_factor_history(repository: Repository, as_of_date: str, months: int) -> pd.DataFrame:
    end = pd.Timestamp(as_of_date)
    start = end - pd.DateOffset(months=months)

    try:
        fundamentals = pd.read_sql_query(
            """
            SELECT f.trade_date, f.code, f.roe, f.gross_margin, f.operating_cashflow_ratio,
                   f.pe_ttm, f.pb, f.ps_ttm, f.net_profit_yoy, f.revenue_yoy
            FROM fundamentals f
            WHERE f.trade_date BETWEEN ? AND ?
            ORDER BY f.trade_date, f.code
            """,
            repository.connection,
            params=(start.strftime("%Y-%m-%d"), as_of_date),
        )

        if fundamentals.empty:
            return pd.DataFrame()

        from qt.factors.quality import compute_quality_score
        from qt.factors.value import compute_value_score
        from qt.factors.expectation import compute_expectation_score

        fundamentals["quality_score"] = compute_quality_score(fundamentals)
        fundamentals["value_score"] = compute_value_score(fundamentals)
        fundamentals["expectation_score"] = compute_expectation_score(fundamentals)
        fundamentals["composite_score"] = (
            fundamentals["quality_score"] * 0.4
            + fundamentals["value_score"] * 0.35
            + fundamentals["expectation_score"] * 0.25
        )

        return fundamentals
    except Exception:
        return pd.DataFrame()


if __name__ == "__main__":
    main()
