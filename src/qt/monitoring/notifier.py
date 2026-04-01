from __future__ import annotations

import os

import requests

from qt.common.logger import get_logger

logger = get_logger(__name__)


class Notifier:
    def __init__(self, sendkey: str | None = None) -> None:
        self.sendkey = sendkey or os.environ.get("SERVERCHAN_SENDKEY", "")

    def send(self, title: str, message: str) -> bool:
        logger.info("通知: %s | %s", title, message)
        if not self.sendkey:
            logger.warning("SERVERCHAN_SENDKEY 未配置，跳过推送")
            return False
        try:
            url = f"https://sctapi.ftqq.com/{self.sendkey}.send"
            resp = requests.post(url, data={"title": title, "desp": message}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0:
                    logger.info("Server酱推送成功")
                    return True
                logger.warning("Server酱返回错误: %s", data)
            else:
                logger.warning("Server酱HTTP错误: %s", resp.status_code)
        except Exception as e:
            logger.error("Server酱推送异常: %s", e)
        return False

    def send_trade_alert(self, action: str, code: str, shares: int, price: float) -> bool:
        title = f"交易提醒: {action} {code}"
        message = f"**{action}** {code}\n- 数量: {shares}\n- 价格: {price:.2f}"
        return self.send(title, message)

    def send_risk_alert(self, alert_type: str, details: str) -> bool:
        title = f"风控预警: {alert_type}"
        return self.send(title, details)

    def send_daily_summary(self, nav: float, cash: float, positions: int, pnl_pct: float) -> bool:
        title = "每日持仓汇总"
        message = (
            f"- 组合净值: {nav:.2f}\n"
            f"- 可用现金: {cash:.2f}\n"
            f"- 持仓数量: {positions}\n"
            f"- 当日盈亏: {pnl_pct:.2%}"
        )
        return self.send(title, message)
