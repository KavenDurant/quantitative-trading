from __future__ import annotations

import time

import akshare as ak
import pandas as pd

from qt.common.logger import get_logger
from qt.data.ingest.universe_builder import RawInstrument
from qt.data.providers.mock_provider import MockDataset, load_mock_dataset
from qt.data.storage.repository import DailyPrice, FundamentalSnapshot, ValuationSnapshot

logger = get_logger(__name__)


class AkshareProvider:
    """基于 AkShare 的真实数据提供者

    支持获取：
    - 股票列表和实时行情
    - 日K线历史数据
    - 财务指标（ROE、毛利率、营收增速等）
    - 估值数据（PE、PB、PS、股息率等）
    """

    def load_dataset(self, as_of_date: str) -> MockDataset:
        return self.load_historical_dataset(as_of_date, as_of_date)

    def load_historical_dataset(self, start_date: str, end_date: str) -> MockDataset:
        base = load_mock_dataset(end_date)
        try:
            instruments = self._load_instruments()
            # 只取主板股票
            main_board = [i for i in instruments if i.board == "main" and not i.is_st]
            target_codes = [item.code for item in main_board[:30]]

            # 获取基本面数据
            fundamentals = self._load_fundamentals(target_codes, end_date)

            # 获取历史价格
            prices = self._load_prices(target_codes, start_date, end_date)

            price_map = {}
            for code in target_codes:
                subset = [item for item in prices if item.code == code]
                if subset:
                    price_map[code] = subset[-1].close
            price_map.update({code: value for code, value in base.latest_prices.items() if code not in price_map})

            return MockDataset(
                instruments=main_board or base.instruments,
                fundamentals=fundamentals or base.fundamentals,
                latest_prices=price_map,
            )
        except Exception as e:
            logger.warning("AkshareProvider 加载数据失败: %s, 使用 mock 数据", e)
            return base

    def safe_load_prices(self, codes: list[str], start_date: str, end_date: str) -> list[DailyPrice]:
        try:
            return self._load_prices(codes, start_date, end_date)
        except Exception:
            from qt.data.providers.mock_history import load_mock_prices
            return [item for item in load_mock_prices(start_date, end_date) if item.code in set(codes)]

    def _load_instruments(self) -> list[RawInstrument]:
        """获取 A 股股票列表"""
        try:
            frame = ak.stock_zh_a_spot_em()
        except Exception:
            return load_mock_dataset("2025-12-31").instruments

        records: list[RawInstrument] = []
        for row in frame.to_dict("records"):
            code = str(row.get("代码", "")).zfill(6)
            name = str(row.get("名称", ""))
            exchange = "SH" if code.startswith(("600", "601", "603", "605", "688")) else "SZ"
            board = "main"
            if code.startswith("300"):
                board = "gem"
            elif code.startswith("688"):
                board = "star"
            records.append(
                RawInstrument(
                    code=code,
                    name=name,
                    list_date="2000-01-01",
                    exchange=exchange,
                    board=board,
                    is_st="ST" in name,
                    is_suspended=False,
                )
            )
        logger.info("Akshare 获取股票列表: %d 只", len(records))
        return records

    def _load_prices(self, codes: list[str], start_date: str, end_date: str) -> list[DailyPrice]:
        """获取日K线历史数据"""
        rows: list[DailyPrice] = []
        for code in codes:
            try:
                frame = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                    adjust="qfq",
                )
                if frame.empty:
                    continue
                for item in frame.to_dict("records"):
                    rows.append(
                        DailyPrice(
                            trade_date=pd.to_datetime(item["日期"]).strftime("%Y-%m-%d"),
                            code=code,
                            open=float(item.get("开盘", 0)),
                            high=float(item.get("最高", 0)),
                            low=float(item.get("最低", 0)),
                            close=float(item["收盘"]),
                            volume=float(item.get("成交量", 0)),
                            amount=float(item.get("成交额", 0)),
                            turnover=float(item.get("换手率", 0)),
                        )
                    )
                time.sleep(0.1)
            except Exception as e:
                logger.warning("获取 %s 价格失败: %s", code, e)

        logger.info("Akshare 获取价格: %d 条记录", len(rows))
        return rows

    def _load_fundamentals(self, codes: list[str], as_of_date: str) -> list[FundamentalSnapshot]:
        """获取基本面数据：ROE、毛利率、营收增速、净利润增速、PE/PB 等

        使用 AkShare 接口：
        - stock_financial_abstract: 财务摘要指标
        - stock_a_lg_indicator: 个股主要指标
        """
        results: list[FundamentalSnapshot] = []

        # 尝试批量获取财务指标
        try:
            # 获取全市场主要财务指标
            indicator_df = ak.stock_a_lg_indicator(symbol="全部")
            indicator_map = {}
            for _, row in indicator_df.iterrows():
                code = str(row.get("股票代码", "")).zfill(6)
                indicator_map[code] = row

            for code in codes:
                row = indicator_map.get(code)
                if row is None:
                    continue

                results.append(FundamentalSnapshot(
                    as_of_date=as_of_date,
                    code=code,
                    roe=self._safe_float(row.get("净资产收益率", 0)),
                    gross_margin=self._safe_float(row.get("销售毛利率", 0)),
                    operating_cashflow_ratio=self._safe_float(row.get("净利润现金含量", 0)),
                    pe_ttm=self._safe_float(row.get("市盈率", 0)),
                    pb=self._safe_float(row.get("市净率", 0)),
                    ps_ttm=self._safe_float(row.get("市销率", 0)),
                    net_profit_yoy=self._safe_float(row.get("净利润同比增长率", 0)),
                    revenue_yoy=self._safe_float(row.get("营业收入同比增长率", 0)),
                ))
        except Exception as e:
            logger.warning("批量获取财务指标失败: %s, 尝试单独获取", e)
            # 降级为单独获取
            for code in codes:
                try:
                    snapshot = self._load_single_fundamental(code, as_of_date)
                    if snapshot:
                        results.append(snapshot)
                    time.sleep(0.2)
                except Exception as ex:
                    logger.warning("获取 %s 基本面失败: %s", code, ex)

        logger.info("Akshare 获取基本面: %d 条", len(results))
        return results

    def _load_single_fundamental(self, code: str, as_of_date: str) -> FundamentalSnapshot | None:
        """获取单只股票的基本面数据"""
        try:
            # 尝试获取财务摘要
            df = ak.stock_financial_abstract(stock=code)
            if df.empty:
                return None

            # 取最近一期数据
            latest = df.iloc[0]

            return FundamentalSnapshot(
                as_of_date=as_of_date,
                code=code,
                roe=self._safe_float(latest.get("净资产收益率", 0)),
                gross_margin=self._safe_float(latest.get("销售毛利率", 0)),
                operating_cashflow_ratio=0.0,  # 需要单独计算
                pe_ttm=self._safe_float(latest.get("市盈率", 0)),
                pb=self._safe_float(latest.get("市净率", 0)),
                ps_ttm=0.0,
                net_profit_yoy=self._safe_float(latest.get("净利润同比增长率", 0)),
                revenue_yoy=self._safe_float(latest.get("营业收入同比增长率", 0)),
            )
        except Exception:
            return None

    def load_valuation(self, codes: list[str], as_of_date: str) -> list[ValuationSnapshot]:
        """获取估值数据：PE、PB、PS、股息率等"""
        results: list[ValuationSnapshot] = []

        try:
            # 批量获取估值指标
            df = ak.stock_a_lg_indicator(symbol="全部")
            for _, row in df.iterrows():
                code = str(row.get("股票代码", "")).zfill(6)
                if code not in codes:
                    continue

                results.append(ValuationSnapshot(
                    trade_date=as_of_date,
                    code=code,
                    pe_ttm=self._safe_float(row.get("市盈率", 0)),
                    pb=self._safe_float(row.get("市净率", 0)),
                    ps_ttm=self._safe_float(row.get("市销率", 0)),
                    pcf_ttm=self._safe_float(row.get("市现率", 0)),
                    dividend_yield=self._safe_float(row.get("股息率", 0)),
                    total_mv=self._safe_float(row.get("总市值", 0)),
                    circ_mv=self._safe_float(row.get("流通市值", 0)),
                ))
        except Exception as e:
            logger.warning("获取估值数据失败: %s", e)

        return results

    @staticmethod
    def _safe_float(val) -> float:
        """安全转换为浮点数"""
        if val is None:
            return 0.0
        try:
            v = float(val)
            return v if v == v else 0.0  # NaN check
        except (ValueError, TypeError):
            return 0.0
