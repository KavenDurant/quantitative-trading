"""BaoStock 数据提供者 — 获取真实A股历史数据

baostock 是免费开源的证券数据接口，数据来源于证券交易所。
支持：日K线(OHLCV)、基本面(ROE/毛利率/PE/PB等)、股票列表。
"""
from __future__ import annotations

import time

import baostock as bs
import pandas as pd

from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import RawInstrument
from qt.data.providers.mock_provider import MockDataset
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot

logger = get_logger(__name__)


def _login() -> None:
    lg = bs.login()
    if lg.error_code != "0":
        logger.error("baostock login failed: %s", lg.error_msg)
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")


def _logout() -> None:
    bs.logout()


def _to_bs_code(code: str) -> str:
    """内部代码转 baostock 格式: 600519 -> sh.600519"""
    if code.startswith("6"):
        return f"sh.{code}"
    return f"sz.{code}"


def _from_bs_code(bs_code: str) -> tuple[str, str]:
    """baostock 格式转内部: sh.600519 -> ('600519', 'SH')"""
    parts = bs_code.split(".")
    exchange = "SH" if parts[0] == "sh" else "SZ"
    return parts[1], exchange


def _query_to_list(rs) -> list[list[str]]:
    data = []
    while (rs.error_code == "0") and rs.next():
        data.append(rs.get_row_data())
    return data


class BaostockProvider:
    """基于 baostock 的真实数据提供者"""

    def load_dataset(self, as_of_date: str) -> MockDataset:
        return self.load_historical_dataset(as_of_date, as_of_date)

    def load_historical_dataset(
        self, start_date: str, end_date: str
    ) -> MockDataset:
        _login()
        try:
            instruments = self._load_instruments(end_date)
            main_board = [
                i for i in instruments
                if i.board == "main" and not i.is_st
            ]
            codes = [i.code for i in main_board]
            fundamentals = self._load_fundamentals(codes, end_date)
            latest_prices = self._load_prices(codes, end_date, end_date)
            price_map = {}
            for p in latest_prices:
                price_map[p.code] = p.close
            return MockDataset(
                instruments=main_board,
                fundamentals=fundamentals,
                latest_prices=price_map,
            )
        finally:
            _logout()

    def safe_load_prices(
        self, codes: list[str], start_date: str, end_date: str
    ) -> list[DailyPrice]:
        _login()
        try:
            return self._load_prices(codes, start_date, end_date)
        finally:
            _logout()

    def _load_instruments(self, as_of_date: str) -> list[RawInstrument]:
        """获取A股股票列表"""
        rs = bs.query_stock_basic(code_name="", code="")
        # fields: code, code_name, ipoDate, outDate, type, status
        data = _query_to_list(rs)
        instruments = []
        for row in data:
            bs_code = row[0]      # sh.600519
            name = row[1]         # code_name
            ipo_date = row[2]     # ipoDate
            stock_type = row[4]   # type: 1=股票 2=指数 3=其他
            status = row[5]       # status: 1=上市 0=退市

            if stock_type != "1" or status != "1":
                continue

            code, exchange = _from_bs_code(bs_code)

            # 判断板块
            board = "main"
            if code.startswith("300") or code.startswith("301"):
                board = "gem"
            elif code.startswith("688"):
                board = "star"
            elif code.startswith(("8", "4")):
                board = "bse"

            is_st = "ST" in name or "st" in name

            instruments.append(RawInstrument(
                code=code,
                name=name,
                list_date=ipo_date,
                exchange=exchange,
                board=board,
                is_st=is_st,
                is_suspended=False,
            ))

        logger.info("baostock 获取股票列表: %d 只", len(instruments))
        return instruments

    def _load_fundamentals(
        self, codes: list[str], as_of_date: str
    ) -> list[FundamentalSnapshot]:
        """获取基本面数据: ROE、毛利率、PE、PB 等"""
        year = as_of_date[:4]
        quarter = self._date_to_quarter(as_of_date)
        results = []
        total = len(codes)

        for idx, code in enumerate(codes, start=1):
            try:
                if idx == 1 or idx % 200 == 0 or idx == total:
                    logger.info("baostock 基本面进度: %d/%d", idx, total)
                bs_code = _to_bs_code(code)

                # 盈利能力
                rs_profit = bs.query_profit_data(
                    code=bs_code, year=int(year), quarter=quarter
                )
                profit_data = _query_to_list(rs_profit)

                roe = 0.0
                gross_margin = 0.0
                net_profit_yoy = 0.0

                if profit_data:
                    row = profit_data[-1]
                    roe = self._safe_float(row[4])  # roeAvg
                    gross_margin = self._safe_float(row[3])  # gpMargin
                    net_profit_yoy = self._safe_float(row[6])  # netProfit yoy

                # 成长能力
                rs_growth = bs.query_growth_data(
                    code=bs_code, year=int(year), quarter=quarter
                )
                growth_data = _query_to_list(rs_growth)

                revenue_yoy = 0.0
                if growth_data:
                    row = growth_data[-1]
                    revenue_yoy = self._safe_float(row[3])  # YOYEquity

                # 估值 — 从日K线获取
                pe_ttm, pb, ps_ttm = self._get_valuation(
                    bs_code, as_of_date
                )

                results.append(FundamentalSnapshot(
                    as_of_date=as_of_date,
                    code=code,
                    roe=roe,
                    gross_margin=gross_margin,
                    operating_cashflow_ratio=0.0,
                    pe_ttm=pe_ttm,
                    pb=pb,
                    ps_ttm=ps_ttm,
                    net_profit_yoy=net_profit_yoy,
                    revenue_yoy=revenue_yoy,
                ))
                time.sleep(0.1)  # 避免请求过快

            except Exception as e:
                logger.warning("获取 %s 基本面失败: %s", code, e)

        logger.info("baostock 获取基本面: %d 只", len(results))
        return results

    def _get_valuation(
        self, bs_code: str, as_of_date: str
    ) -> tuple[float, float, float]:
        """从日K线获取 PE/PB"""
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,peTTM,pbMRQ,psTTM",
            start_date=as_of_date,
            end_date=as_of_date,
            frequency="d",
        )
        data = _query_to_list(rs)
        if data:
            row = data[-1]
            return (
                self._safe_float(row[1]),
                self._safe_float(row[2]),
                self._safe_float(row[3]),
            )
        return 0.0, 0.0, 0.0

    def _load_prices(
        self, codes: list[str], start_date: str, end_date: str
    ) -> list[DailyPrice]:
        """获取日K线 OHLCV"""
        all_prices: list[DailyPrice] = []
        total = len(codes)

        for idx, code in enumerate(codes, start=1):
            try:
                if idx == 1 or idx % 200 == 0 or idx == total:
                    logger.info("baostock 价格进度: %d/%d", idx, total)
                bs_code = _to_bs_code(code)
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,turn",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2",  # 前复权
                )
                data = _query_to_list(rs)

                for row in data:
                    close_val = self._safe_float(row[4])
                    if close_val <= 0:
                        continue
                    all_prices.append(DailyPrice(
                        trade_date=row[0],
                        code=code,
                        open=self._safe_float(row[1]),
                        high=self._safe_float(row[2]),
                        low=self._safe_float(row[3]),
                        close=close_val,
                        volume=self._safe_float(row[5]),
                        amount=self._safe_float(row[6]),
                        turnover=self._safe_float(row[7]),
                    ))
                time.sleep(0.05)

            except Exception as e:
                logger.warning("获取 %s 价格失败: %s", code, e)

        logger.info(
            "baostock 获取价格: %d 只股票 %d 条记录",
            len(codes), len(all_prices),
        )
        return all_prices

    @staticmethod
    def _date_to_quarter(date_str: str) -> int:
        month = int(date_str[5:7])
        if month <= 3:
            return 1
        if month <= 6:
            return 2
        if month <= 9:
            return 3
        return 4

    @staticmethod
    def _safe_float(val: str) -> float:
        try:
            v = float(val)
            return v if v == v else 0.0  # NaN check
        except (ValueError, TypeError):
            return 0.0
