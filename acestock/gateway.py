import asyncio
from typing import Dict, Any, List
from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    SubscribeRequest, OrderRequest, CancelRequest, OrderData, HistoryRequest, BarData
)

from .market_data import MarketData
from .trade_data import TradeData


class AcestockGateway(BaseGateway):
    gateway_name = "acestock"
    default_setting: Dict[str, Any] = {
        "broker": "universal_client",
        "user": "",
        "password": "",
        "exe_path": "",
        "comm_password": "",
        "host": "",
        "port": "1430"
    }
    exchanges: List[Exchange] = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str = gateway_name):
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md = MarketData(self)
        self.td = TradeData(self)

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        self.md.connect()
        self.td.connect(setting)

    def close(self) -> None:
        """关闭接口"""
        self.td.close()
        self.md.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req not in self.md.api_subscribe_req_list:
            self.md.api_subscribe_req_list.append(req.symbol)
            coroutine = self.md.query_tick(req)
            asyncio.run_coroutine_threadsafe(coroutine, self.md.loop)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td.query_account()

    def query_position(self) -> None:
        self.td.query_position()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        Query bar history data.
        """
        return self.md.query_history(req)
