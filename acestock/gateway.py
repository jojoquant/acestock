import asyncio
from asyncio import ProactorEventLoop
from typing import Dict, Any, List
from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    SubscribeRequest, OrderRequest, CancelRequest, OrderData, HistoryRequest, BarData
)

from acestock.md import MarketDataMD
from acestock.td import TradeDataTD


class AcestockGateway(BaseGateway):
    default_name: str = "acestock"
    default_setting: Dict[str, Any] = {
        "broker": "universal_client",
        "user": "",
        "password": "",
        "exe_path": "",
        "comm_password": "",
        "host": "",
        "port": "1430",
        "update_bestip": "",
        "last_best_ip": "",
        "last_best_port": ""
    }
    exchanges: List[Exchange] = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md = MarketDataMD(self)
        self.td = TradeDataTD(self)

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: Dict[str, str]) -> None:
        self.md.connect(setting)
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
            if isinstance(self.md.loop, ProactorEventLoop):
                asyncio.run_coroutine_threadsafe(coroutine, self.md.loop)
            else:
                self.write_log(f"self.md.loop is {self.md.loop}, cancel req subcribe")
                self.write_log(f"req: {req}")

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


if __name__ == '__main__':
    from vnpy.trader.utility import load_json
    from joconst.object import Product, ContractData, Interval
    from datetime import datetime
    from vnpy.trader.datafeed import get_datafeed
    import dolphindb as ddb

    event_engine = EventEngine()
    filename = f"connect_{AcestockGateway.default_name}.json"
    setting_dict = load_json(filename)

    # datafeed 里面写了 while 循环 concate 拼接数据
    vnpy_jotdx_datafeed = get_datafeed()
    vnpy_jotdx_datafeed.init()

    # acestock 和 vnpy_jotdx 里面都有获取全部的 股票市场 合约数据的方法
    # vnpy_jotdx datafeed 里面 多了期货合约的获取, 但是jotdx底层这里获取有问题, 不同服务器获取不全
    # 所以期货合约数据主要还是来源于 ctp 或者 tts 这些 gateway 的行情接口
    acestock_gateway = AcestockGateway(event_engine=event_engine, gateway_name=AcestockGateway.default_name)
    acestock_gateway.md.connect(setting_dict)

    v: ContractData
    for _, v in acestock_gateway.md.contracts_dict[Product.BOND].items():
        req = HistoryRequest(
            symbol=v.symbol,
            exchange=v.exchange,
            start=datetime(2000, 1, 1),
            end=datetime.now(),
            interval=Interval.MINUTE
        )
        df = acestock_gateway.md.datafeed.query_bar_df_history(req=req)
        print(1)
    print(1)
