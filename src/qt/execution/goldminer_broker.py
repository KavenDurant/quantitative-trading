"""掘金量化 API 封装

基于掘金量化 Python SDK (gm.api) 封装交易接口。
文档参考:
- 快速开始: https://myquant.cn/docs2/sdk/python/快速开始.html
- 交易函数: https://myquant.cn/docs2/sdk/python/API介绍/交易函数.html
- 交易查询: https://myquant.cn/docs2/sdk/python/API介绍/交易查询函数.html
- 基本函数: https://myquant.cn/docs2/sdk/python/API介绍/基本函数.html

使用前需要:
1. pip install gm
2. 在 myquant.cn 注册账号并获取 Token
3. 创建模拟交易账户获取 account_id
4. 配置环境变量 GM_API_TOKEN 和 GM_ACCOUNT_ID
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from qt.common.logger import get_logger

logger = get_logger(__name__)

# 掘金标的代码格式: SHSE.600519 (上交所) / SZSE.000651 (深交所)
EXCHANGE_MAP = {"SH": "SHSE", "SZ": "SZSE"}


def to_gm_symbol(code: str, exchange: str = "") -> str:
    """将内部代码转为掘金格式: 600519 -> SHSE.600519"""
    if "." in code and code.split(".")[0] in ("SHSE", "SZSE"):
        return code
    if exchange:
        prefix = EXCHANGE_MAP.get(exchange, "SHSE")
        return f"{prefix}.{code}"
    if code.startswith("6"):
        return f"SHSE.{code}"
    return f"SZSE.{code}"


def from_gm_symbol(gm_symbol: str) -> tuple[str, str]:
    """掘金格式转内部格式: SHSE.600519 -> ('600519', 'SH')"""
    parts = gm_symbol.split(".")
    if len(parts) == 2:
        exchange = "SH" if parts[0] == "SHSE" else "SZ"
        return parts[1], exchange
    return gm_symbol, ""


@dataclass
class GoldMinerPosition:
    code: str
    exchange: str
    volume: int
    available: int
    cost: float
    current_price: float
    market_value: float
    pnl: float


@dataclass
class GoldMinerOrder:
    order_id: str
    code: str
    side: str  # "buy" or "sell"
    volume: int
    price: float
    filled_volume: int
    status: str


@dataclass
class GoldMinerAccount:
    cash: float
    frozen_cash: float
    total_assets: float
    market_value: float


class GoldMinerBroker:
    """掘金量化 API 封装

    核心 API 调用:
    - gm.api.set_token(token)
    - gm.api.order_volume(symbol, volume, side, order_type, position_effect, price)
    - gm.api.order_cancel(wait_cancel_orders)
    - gm.api.get_cash()
    - gm.api.get_position(symbol)
    - gm.api.get_orders()
    - gm.api.current(symbols)
    """

    def __init__(
        self,
        token: str | None = None,
        account_id: str | None = None,
    ) -> None:
        self.token = token or os.environ.get("GM_API_TOKEN", "")
        self.account_id = account_id or os.environ.get("GM_ACCOUNT_ID", "")
        self._gm = None
        self._initialized = False

    def _ensure_init(self) -> None:
        if self._initialized:
            return
        try:
            from gm.api import set_token
            set_token(self.token)
            self._initialized = True
            logger.info("掘金量化 SDK 初始化成功")
        except ImportError:
            logger.error("gm 包未安装，请运行: pip install gm")
            raise
        except Exception as e:
            logger.error("掘金量化初始化失败: %s", e)
            raise

    def get_account(self) -> GoldMinerAccount:
        self._ensure_init()
        from gm.api import get_cash
        cash_info = get_cash()
        return GoldMinerAccount(
            cash=float(cash_info.available),
            frozen_cash=float(cash_info.frozen),
            total_assets=float(cash_info.nav),
            market_value=float(cash_info.nav - cash_info.available - cash_info.frozen),
        )

    def get_cash(self) -> float:
        return self.get_account().cash

    def get_total_assets(self) -> float:
        return self.get_account().total_assets

    def get_positions(self) -> list[GoldMinerPosition]:
        self._ensure_init()
        from gm.api import get_position
        positions = get_position()
        result = []
        for pos in positions:
            code, exchange = from_gm_symbol(pos.symbol)
            result.append(GoldMinerPosition(
                code=code,
                exchange=exchange,
                volume=int(pos.volume),
                available=int(pos.available),
                cost=float(pos.vwap),
                current_price=float(pos.price),
                market_value=float(pos.market_value),
                pnl=float(pos.fpnl),
            ))
        return result

    def get_position_by_code(self, code: str) -> GoldMinerPosition | None:
        positions = self.get_positions()
        for pos in positions:
            if pos.code == code:
                return pos
        return None

    def get_current_price(self, code: str) -> float:
        self._ensure_init()
        from gm.api import current
        symbol = to_gm_symbol(code)
        tick = current(symbol)
        if tick:
            return float(tick[0].price) if isinstance(tick, list) else float(tick.price)
        return 0.0

    def buy_limit(self, code: str, volume: int, price: float) -> str | None:
        """限价买入

        gm.api.order_volume(symbol, volume, side, order_type, position_effect, price)
        - side: OrderSide_Buy = 1
        - order_type: OrderType_Limit = 1
        - position_effect: PositionEffect_Open = 1
        """
        self._ensure_init()
        from gm.api import order_volume
        symbol = to_gm_symbol(code)
        try:
            order = order_volume(
                symbol=symbol,
                volume=volume,
                side=1,           # OrderSide_Buy
                order_type=1,     # OrderType_Limit
                position_effect=1, # PositionEffect_Open
                price=price,
            )
            if order:
                order_id = order[0].cl_ord_id if isinstance(order, list) else order.cl_ord_id
                logger.info("限价买入: %s %d股 @ %.2f 订单号=%s", code, volume, price, order_id)
                return order_id
        except Exception as e:
            logger.error("限价买入失败: %s %d股 @ %.2f error=%s", code, volume, price, e)
        return None

    def sell_limit(self, code: str, volume: int, price: float) -> str | None:
        """限价卖出

        - side: OrderSide_Sell = 2
        - order_type: OrderType_Limit = 1
        - position_effect: PositionEffect_Close = 2
        """
        self._ensure_init()
        from gm.api import order_volume
        symbol = to_gm_symbol(code)
        try:
            order = order_volume(
                symbol=symbol,
                volume=volume,
                side=2,           # OrderSide_Sell
                order_type=1,     # OrderType_Limit
                position_effect=2, # PositionEffect_Close
                price=price,
            )
            if order:
                order_id = order[0].cl_ord_id if isinstance(order, list) else order.cl_ord_id
                logger.info("限价卖出: %s %d股 @ %.2f 订单号=%s", code, volume, price, order_id)
                return order_id
        except Exception as e:
            logger.error("限价卖出失败: %s %d股 @ %.2f error=%s", code, volume, price, e)
        return None

    def sell_market(self, code: str, volume: int) -> str | None:
        """市价卖出（止损用）

        - side: OrderSide_Sell = 2
        - order_type: OrderType_Market = 2
        - position_effect: PositionEffect_Close = 2
        """
        self._ensure_init()
        from gm.api import order_volume
        symbol = to_gm_symbol(code)
        try:
            order = order_volume(
                symbol=symbol,
                volume=volume,
                side=2,           # OrderSide_Sell
                order_type=2,     # OrderType_Market
                position_effect=2, # PositionEffect_Close
                price=0,
            )
            if order:
                order_id = order[0].cl_ord_id if isinstance(order, list) else order.cl_ord_id
                logger.info("市价止损卖出: %s %d股 订单号=%s", code, volume, order_id)
                return order_id
        except Exception as e:
            logger.error("市价止损卖出失败: %s %d股 error=%s", code, volume, e)
        return None

    def cancel_all_pending(self) -> int:
        """撤销所有未成交委托"""
        self._ensure_init()
        from gm.api import get_orders, order_cancel
        try:
            orders = get_orders()
            pending = [o for o in orders if o.status in (1, 3)]  # 1=待报 3=已报待成
            if pending:
                order_cancel(pending)
                logger.info("撤销 %d 笔未成交委托", len(pending))
            return len(pending)
        except Exception as e:
            logger.error("撤单失败: %s", e)
            return 0

    def get_pending_orders(self) -> list[GoldMinerOrder]:
        self._ensure_init()
        from gm.api import get_orders
        try:
            orders = get_orders()
            result = []
            for o in orders:
                code, _ = from_gm_symbol(o.symbol)
                result.append(GoldMinerOrder(
                    order_id=str(o.cl_ord_id),
                    code=code,
                    side="buy" if o.side == 1 else "sell",
                    volume=int(o.volume),
                    price=float(o.price),
                    filled_volume=int(o.filled_volume),
                    status=str(o.status),
                ))
            return result
        except Exception as e:
            logger.error("查询委托失败: %s", e)
            return []
