"""基于掘金量化 SDK (gm) 的真实数据提供者

通过掘金终端数据服务批量获取 A 股真实行情数据（极快）。
基本面数据通过 baostock 批量获取日K线估值数据（PE/PB/PS），
ROE/毛利率等季频指标取全部主板股票。
"""
from __future__ import annotations

import os
import time

import pandas as pd
from gm.api import get_instruments, history, set_token

from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import RawInstrument
from qt.data.providers.mock_provider import MockDataset, load_mock_dataset
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot

logger = get_logger(__name__)


def _get_token() -> str:
    return os.environ.get("GM_API_TOKEN", "")


def _code_to_symbol(code: str) -> str:
    """600519 -> SHSE.600519"""
    if code.startswith(("6",)):
        return f"SHSE.{code}"
    return f"SZSE.{code}"


def _symbol_to_code(symbol: str) -> str:
    """SHSE.600519 -> 600519"""
    return symbol.split(".")[-1]


class GmProvider:
    """掘金量化 SDK 数据提供者

    行情 + 估值: gm history 批量获取（秒级）
    ROE/毛利率等季频指标: baostock 批量获取
    """

    def __init__(self) -> None:
        token = _get_token()
        if token:
            set_token(token)
            logger.info("GmProvider: token 已设置")
        else:
            logger.warning("GmProvider: 未配置 GM_API_TOKEN")

    # ---- dataset 接口（供 backfill_data 调用）----

    def load_dataset(self, as_of_date: str) -> MockDataset:
        return self.load_historical_dataset(as_of_date, as_of_date)

    def load_historical_dataset(self, start_date: str, end_date: str) -> MockDataset:
        base = load_mock_dataset(end_date)
        try:
            instruments = self._load_instruments()
            main_board = [i for i in instruments if i.board == "main" and not i.is_st]
        except Exception as e:
            logger.warning("GmProvider 获取股票列表失败: %s", e)
            main_board = base.instruments

        target_codes = [item.code for item in main_board]

        # 基本面: baostock 获取全部主板
        fundamentals = self._load_fundamentals_via_baostock(target_codes, end_date)

        # 最新价: gm 批量获取
        prices = self._load_latest_prices_batch(target_codes, end_date)

        return MockDataset(
            instruments=main_board or base.instruments,
            fundamentals=fundamentals or base.fundamentals,
            latest_prices=prices or base.latest_prices,
        )

    def safe_load_prices(self, codes: list[str], start_date: str, end_date: str) -> list[DailyPrice]:
        try:
            return self._load_prices_batch(codes, start_date, end_date)
        except Exception as e:
            logger.warning("GmProvider safe_load_prices 失败: %s", e)
            return []

    # ---- 股票列表 ----

    def _load_instruments(self) -> list[RawInstrument]:
        """获取沪深主板 A 股列表"""
        instruments = get_instruments(
            exchanges="SHSE,SZSE", sec_types=1,
            fields="symbol,sec_name,listed_date",
            df=True,
        )
        if instruments is None or instruments.empty:
            return load_mock_dataset("2025-12-31").instruments

        records: list[RawInstrument] = []
        for _, row in instruments.iterrows():
            symbol = str(row.get("symbol", ""))
            code = _symbol_to_code(symbol)
            name = str(row.get("sec_name", ""))
            list_date_raw = row.get("listed_date", "")
            list_date = "2000-01-01"
            if list_date_raw is not None:
                try:
                    list_date = pd.Timestamp(list_date_raw).strftime("%Y-%m-%d")
                except Exception:
                    list_date = "2000-01-01"
            exchange = "SH" if code.startswith("6") else "SZ"
            board = "main"
            if code.startswith("300") or code.startswith("301"):
                board = "gem"
            elif code.startswith("688"):
                board = "star"
            is_st = "ST" in name or "st" in name
            records.append(
                RawInstrument(
                    code=code, name=name, list_date=list_date,
                    exchange=exchange, board=board, is_st=is_st,
                    is_suspended=False,
                )
            )
        logger.info("GmProvider 获取股票列表: %d 只", len(records))
        return records

    # ---- 价格数据（gm 批量查询） ----

    def _load_prices_batch(self, codes: list[str], start_date: str, end_date: str) -> list[DailyPrice]:
        """批量获取日K线历史数据，使用 gm 逗号分隔多 symbol"""
        rows: list[DailyPrice] = []
        batch_size = 500

        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i + batch_size]
            symbols = ",".join(_code_to_symbol(c) for c in batch_codes)
            try:
                df = history(
                    symbol=symbols, frequency="1d",
                    start_time=start_date, end_time=end_date,
                    fields="symbol,open,high,low,close,volume,amount,bob",
                    df=True, adjust=1,
                )
                if df is None or df.empty:
                    continue
                for _, item in df.iterrows():
                    sym = str(item.get("symbol", ""))
                    code = _symbol_to_code(sym) if "." in sym else sym
                    trade_date = pd.Timestamp(item["bob"]).strftime("%Y-%m-%d")
                    rows.append(
                        DailyPrice(
                            trade_date=trade_date, code=code,
                            open=float(item.get("open", 0)),
                            high=float(item.get("high", 0)),
                            low=float(item.get("low", 0)),
                            close=float(item["close"]),
                            volume=float(item.get("volume", 0)),
                            amount=float(item.get("amount", 0)),
                            turnover=0.0,
                        )
                    )
            except Exception as e:
                logger.warning("GmProvider 批量获取价格失败 (batch %d-%d): %s", i, i + batch_size, e)

            if i + batch_size < len(codes):
                time.sleep(0.5)

        logger.info("GmProvider 获取价格: %d 条", len(rows))
        return rows

    def _load_latest_prices_batch(self, codes: list[str], as_of_date: str) -> dict[str, float]:
        """批量获取最新收盘价"""
        price_map: dict[str, float] = {}
        batch_size = 500

        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i + batch_size]
            symbols = ",".join(_code_to_symbol(c) for c in batch_codes)
            try:
                df = history(
                    symbol=symbols, frequency="1d",
                    start_time=as_of_date, end_time=as_of_date,
                    fields="symbol,close", df=True,
                )
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        sym = str(row.get("symbol", ""))
                        code = _symbol_to_code(sym) if "." in sym else sym
                        price_map[code] = float(row["close"])
            except Exception:
                pass

        logger.info("GmProvider 获取最新价: %d 只", len(price_map))
        return price_map

    # ---- 基本面数据（baostock 全量获取） ----

    def _load_fundamentals_via_baostock(self, codes: list[str], as_of_date: str) -> list[FundamentalSnapshot]:
        """使用 baostock 获取全部主板基本面数据

        估值部分(PE/PB/PS) 从日K线批量获取，ROE/毛利率等季频指标逐只获取。
        """
        try:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg}")
            try:
                results = self._baostock_fundamentals_batch(codes, as_of_date)
                logger.info("GmProvider: baostock 获取基本面 %d 条", len(results))
                return results
            finally:
                bs.logout()
        except Exception as e:
            logger.warning("GmProvider: baostock 基本面数据获取失败: %s", e)
            return []

    def _baostock_fundamentals_batch(self, codes: list[str], as_of_date: str) -> list[FundamentalSnapshot]:
        """baostock 批量获取基本面 — 估值从日K线批量查，季频指标分批

        profit_data 字段: code, pubDate, statDate, roeAvg, npMargin,
                          gpMargin, netProfit, epsTTM, MBRevenue, ...
        growth_data 字段:  code, pubDate, statDate, YOYEquity, YOYAsset,
                          YOYNI, YOYEPSBasic, YOYPNI
        cashflow 字段:     code, pubDate, statDate, CAToAsset, NCAToAsset, ...,
                          CFOToNP, ...
        """
        import baostock as bs
        from qt.data.providers.baostock_provider import _to_bs_code, _query_to_list as bs_query

        year, quarter = self._latest_reported_quarter(as_of_date)
        results: list[FundamentalSnapshot] = []
        total = len(codes)

        # 阶段1: 从日K线获取估值数据 (peTTM, pbMRQ, psTTM)
        valuation_map: dict[str, dict] = {}
        logger.info("开始获取估值数据 (PE/PB/PS) for %d 只股票...", total)

        for idx, code in enumerate(codes, start=1):
            if idx % 500 == 0 or idx == total:
                logger.info("估值进度: %d/%d", idx, total)
            bs_code = _to_bs_code(code)
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code, "date,peTTM,pbMRQ,psTTM",
                    start_date=as_of_date, end_date=as_of_date,
                    frequency="d",
                )
                data = bs_query(rs)
                if data:
                    row = data[-1]
                    valuation_map[code] = {
                        "pe_ttm": self._safe_float(row[1]),
                        "pb": self._safe_float(row[2]),
                        "ps_ttm": self._safe_float(row[3]),
                    }
            except Exception:
                pass

        logger.info("估值数据: %d 只", len(valuation_map))

        # 阶段2: 季频指标 (ROE, 毛利率, 净利润增速, 营收增速, 现金流)
        logger.info("开始获取季频指标 for %d 只股票 (year=%d Q%d)...", len(codes), year, quarter)

        for idx, code in enumerate(codes, start=1):
            if idx % 500 == 0 or idx == total:
                logger.info("季频进度: %d/%d", idx, total)
            bs_code = _to_bs_code(code)
            val = valuation_map.get(code, {})

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
                    roe = self._safe_float(row[3])  # roeAvg
                    gross_margin = self._safe_float(row[5])  # gpMargin
            except Exception:
                pass

            try:
                rs_growth = bs.query_growth_data(
                    code=bs_code, year=year, quarter=quarter
                )
                growth_data = bs_query(rs_growth)
                if growth_data:
                    row = growth_data[-1]
                    net_profit_yoy = self._safe_float(row[5])  # YOYNI
                    revenue_yoy = self._safe_float(row[7])  # YOYPNI
            except Exception:
                pass

            try:
                rs_cash = bs.query_cash_flow_data(
                    code=bs_code, year=year, quarter=quarter
                )
                cash_data = bs_query(rs_cash)
                if cash_data:
                    row = cash_data[-1]
                    cashflow_ratio = self._safe_float(row[8])  # CFOToNP
            except Exception:
                pass

            results.append(FundamentalSnapshot(
                as_of_date=as_of_date,
                code=code,
                roe=roe,
                gross_margin=gross_margin,
                operating_cashflow_ratio=cashflow_ratio,
                pe_ttm=val.get("pe_ttm", 0.0),
                pb=val.get("pb", 0.0),
                ps_ttm=val.get("ps_ttm", 0.0),
                net_profit_yoy=net_profit_yoy,
                revenue_yoy=revenue_yoy,
            ))

        logger.info("baostock 全量基本面: %d 条", len(results))
        return results

    @staticmethod
    def _latest_reported_quarter(date_str: str) -> tuple[int, int]:
        """返回 date_str 时最近已发布的财报 (year, quarter)

        A股财报披露规则:
        - Q1 (一季报): 最晚 4 月 30 日披露
        - Q2 (中报):   最晚 8 月 31 日披露
        - Q3 (三季报): 最晚 10 月 31 日披露
        - Q4 (年报):   最晚次年 4 月 30 日披露
        """
        year = int(date_str[:4])
        month = int(date_str[5:7])
        if month >= 11:
            return year, 3
        elif month >= 9:
            return year, 2
        elif month >= 5:
            return year, 1
        else:
            return year - 1, 3

    @staticmethod
    def _safe_float(val) -> float:
        if val is None:
            return 0.0
        try:
            v = float(val)
            return v if v == v else 0.0
        except (ValueError, TypeError):
            return 0.0
