from __future__ import annotations

from sqlite3 import Connection

from qt.common.logger import get_logger

logger = get_logger(__name__)


class DataQualityChecker:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.issues: list[str] = []

    def check_instrument_count(self, min_count: int = 100) -> bool:
        row = self.connection.execute("SELECT COUNT(*) FROM instrument_master").fetchone()
        count = row[0] if row else 0
        ok = count >= min_count
        msg = f"股票总数: {count} (最低要求 {min_count})"
        if not ok:
            self.issues.append(msg)
        logger.info("CHECK instrument_count: %s -> %s", msg, "PASS" if ok else "FAIL")
        return ok

    def check_price_count(self, min_count: int = 500) -> bool:
        row = self.connection.execute("SELECT COUNT(*) FROM prices_daily").fetchone()
        count = row[0] if row else 0
        ok = count >= min_count
        msg = f"日行情数据量: {count} (最低要求 {min_count})"
        if not ok:
            self.issues.append(msg)
        logger.info("CHECK price_count: %s -> %s", msg, "PASS" if ok else "FAIL")
        return ok

    def check_latest_date(self) -> str:
        row = self.connection.execute("SELECT MAX(trade_date) FROM prices_daily").fetchone()
        latest = row[0] if row and row[0] else "N/A"
        logger.info("CHECK latest_price_date: %s", latest)
        return latest

    def check_no_st_in_universe(self) -> bool:
        row = self.connection.execute("SELECT COUNT(*) FROM instrument_master WHERE is_st = 1").fetchone()
        count = row[0] if row else 0
        ok = count == 0
        msg = f"ST 股票混入数: {count}"
        if not ok:
            self.issues.append(msg)
        logger.info("CHECK no_st: %s -> %s", msg, "PASS" if ok else "FAIL")
        return ok

    def check_price_distribution(self) -> dict[str, float]:
        row = self.connection.execute(
            "SELECT MIN(close), AVG(close), MAX(close) FROM prices_daily WHERE close > 0"
        ).fetchone()
        if not row or row[0] is None:
            logger.info("CHECK price_distribution: 无数据")
            return {"min": 0, "avg": 0, "max": 0}
        result = {"min": round(row[0], 2), "avg": round(row[1], 2), "max": round(row[2], 2)}
        logger.info("CHECK price_distribution: min=%.2f avg=%.2f max=%.2f", result["min"], result["avg"], result["max"])
        return result

    def run_all(self) -> bool:
        self.issues.clear()
        self.check_instrument_count()
        self.check_price_count()
        self.check_latest_date()
        self.check_no_st_in_universe()
        self.check_price_distribution()
        if self.issues:
            logger.warning("数据质量检查发现 %d 个问题: %s", len(self.issues), self.issues)
            return False
        logger.info("数据质量检查全部通过")
        return True
