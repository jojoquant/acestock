import asyncio
import threading
from asyncio import AbstractEventLoop
from copy import copy
from datetime import datetime
from functools import partial
from typing import Dict, Any
from tzlocal import get_localzone

from easytrader import remoteclient
from jotdx.quotes import Quotes
from jotdx.consts import MARKET_SH, MARKET_SZ

from vnpy.event import EventEngine, EVENT_TIMER
from vnpy.trader.constant import Offset, Status, Exchange, Direction, Product
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import SubscribeRequest, OrderRequest, CancelRequest, PositionData, OrderData, AccountData, \
    ContractData, TickData

# 交易所映射
MARKET2VT: Dict[str, Exchange] = {
    "深A": Exchange.SZSE,
    "沪A": Exchange.SSE,
}


def trans_tick_df_to_tick_data(tick_df, req: SubscribeRequest) -> TickData:
    # buyorsell, 0 buy, 1 sell
    # buyorsell = tick_df['buyorsell'][0]

    return TickData(
        gateway_name="paper",
        symbol=req.symbol,
        exchange=req.exchange,
        datetime=datetime.now(get_localzone()),
        volume=tick_df['vol'][0],
        # num 放到turnover, 因为在bargenerater里面,
        # turnover是累加计算的, open_interest 是不算累加的而取截面的
        turnover=tick_df['num'][0],
        last_price=tick_df['price'][0],
    )


class AcetockGateway(BaseGateway):
    default_setting: Dict[str, Any] = {
        "broker": "universal_client",
        "user": "",
        "password": "",
        "exe_path": "",
        "comm_password": "",
        "host": "",
        "port": "1430"
    }

    def __init__(self, event_engine: EventEngine, gateway_name: str = "acetock"):
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api = Quotes.factory(market='std')
        self.md_api_subscribe_req_list = []
        self.md_thread: threading.Thread = None
        self.loop: AbstractEventLoop = None

        self.td_api = ""
        self.td_api_setting = {}
        self.non_ths_client_list = ['htzq_client', 'ht_client', "gj_client"]
        self.ths_client_list = ['universal_client']

        self.orderid: int = 0

    def connect(self, setting: dict) -> None:

        self.connect_md_api()
        self.connect_td_api(setting)

    def connect_md_api(self):
        self.query_contract()

        try:
            self.loop = asyncio.new_event_loop()  # 在当前线程下创建时间循环，（未启用），在start_loop里面启动它
            self.md_thread = threading.Thread(target=self.start_loop, args=(self.loop,))  # 通过当前线程开启新的线程去启动事件循环
            self.write_log("启动行情线程...")
            self.md_thread.start()
        except:
            self.write_log("行情线程启动出现问题!")

    def start_loop(self, loop):
        asyncio.set_event_loop(loop)
        try:
            self.write_log("行情线程中启动协程 loop ...")
            loop.run_forever()
        except:
            self.write_log("行情线程中启动协程 loop 出现问题!")

    def connect_td_api(self, setting):
        self.td_api_setting = setting
        if setting['broker'] in self.non_ths_client_list:
            self.td_api = remoteclient.use(**setting)
        elif setting['broker'] in self.ths_client_list:
            # 通用同花顺客户端
            self.td_api = remoteclient.use(**setting)
            # remoteclient 同花顺远程输入代码存在问题, 不存在下面的方法
            # self.td_api.enable_type_keys_for_editor()
            self.write_log("同花顺远程输入代码存在问题, 注意测试 buy/sell 功能")
        else:
            # 其他券商专用同花顺客户端
            # 其他券商专用同花顺客户端不支持自动登录，需要先手动登录。
            # 请手动打开并登录客户端后，运用connect函数连接客户端。
            self.write_log("多线程不支持其他券商专用同花顺客户端")
        try:
            self.td_api.prepare(**setting)
            self.write_log("交易服务器连接成功!")
            self.query_account()
            self.query_position()
        except Exception as e:
            self.write_log(f"交易服务器连接失败! {e}")

    def close(self) -> None:
        """关闭接口"""
        self.td_api.exit()
        self.write_log("交易服务器断开连接")
        self.md_api.close()
        self.write_log("行情服务器断开连接")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req not in self.md_api_subscribe_req_list:
            self.md_api_subscribe_req_list.append(req.symbol)
            coroutine = self.query_tick(req)
            asyncio.run_coroutine_threadsafe(coroutine, self.loop)

    async def query_tick(self, req: SubscribeRequest):
        client = Quotes.factory(market='std')
        loop = asyncio.get_event_loop()

        params = {"symbol": req.symbol, "start": 0, "offset": 1}
        last_tick_df = await loop.run_in_executor(None, partial(client.transaction, **params))

        tz = get_localzone()
        tick_datetime = datetime.now(tz)

        start_datetime = datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=9, minute=30, second=0, microsecond=0, tzinfo=tz)
        end_datetime = datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=15, minute=0, second=0, microsecond=0, tzinfo=tz)

        while start_datetime <= tick_datetime <= end_datetime:
            df1 = await loop.run_in_executor(None, partial(client.transaction, **params))
            last_tick_df = last_tick_df.append(df1).drop_duplicates()
            if len(last_tick_df) != 1:
                last_tick_df = df1
                tick = trans_tick_df_to_tick_data(last_tick_df, req)
                self.on_tick(tick)
            await asyncio.sleep(1.5)

            df2 = await loop.run_in_executor(None, partial(client.transaction, **params))
            last_tick_df = last_tick_df.append(df2).drop_duplicates()
            if len(last_tick_df) != 1:
                last_tick_df = df2
                tick = trans_tick_df_to_tick_data(last_tick_df, req)
                self.on_tick(tick)

            await asyncio.sleep(1.5)
            # 这里注意要更新时间
            tick_datetime = datetime.now(tz)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        try:
            if req.offset == Offset.OPEN:

                ret = self.td_api.buy(security=req.symbol, price=req.price, amount=req.volume)[0]
                order_id = ret.get('entrust_no', default="success")

                order = req.create_order_data(order_id, self.gateway_name)
                order.status = Status.ALLTRADED
                self.orders[order_id] = order
                self.gateway.on_order(copy(order))

                if order_id == "success":
                    self.write_log("系统配置未设置为 返回成交回报, 将影响撤单操作")

            elif req.direction == Offset.CLOSE:

                ret = self.td_api.sell(security=req.symbol, price=req.price, amount=req.volume)[0]
                order_id = ret.get('entrust_no', default="success")

                order = req.create_order_data(order_id, self.gateway_name)
                order.status = Status.ALLTRADED
                self.orders[order_id] = order
                self.on_order(copy(order))

                if order_id == "success":
                    self.write_log("系统配置未设置为 返回成交回报, 将影响撤单操作")

        except IOError as e:
            order.status = Status.REJECTED
            self.on_order(order)

            msg: str = f"开仓委托失败，信息：{e}"
            self.write_log(msg)

        finally:
            return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        # TODO
        self.td_api.cancel_entrust(req.orderid)

    def query_account(self) -> None:
        """查询资金"""
        try:
            ret = self.td_api.balance
            if self.td_api_setting['broker'] in self.non_ths_client_list:
                account: AccountData = AccountData(
                    gateway_name=self.gateway_name,
                    accountid=self.td_api_setting['broker'],
                    balance=ret['总资产'],
                    frozen=ret['总资产'] - ret['可用金额']
                )
            elif self.td_api_setting['broker'] in self.ths_client_list:
                account: AccountData = AccountData(
                    gateway_name=self.gateway_name,
                    accountid=ret['资金账号'],
                    balance=ret['总资产'],
                    frozen=ret['总资产'] - ret['可用资金']
                )
            self.on_account(account)
            self.write_log("账户资金查询成功")
        except:
            self.write_log("账户资金查询失败")

    def query_position(self) -> None:
        """查询持仓"""
        try:
            ret_list = self.td_api.position
            for ret in ret_list:
                if self.td_api_setting['broker'] in self.non_ths_client_list:
                    position = PositionData(
                        symbol=str(ret["证券代码"]),
                        exchange=MARKET2VT[ret["交易市场"]] if ret["交易市场"] else Exchange.SSE,
                        direction=Direction.LONG,
                        volume=float(ret["股票余额"]),
                        frozen=float(ret["冻结数量"]),
                        price=float(ret["成本价"]),
                        pnl=float(ret["盈亏"]),
                        yd_volume=float(ret["可用余额"]),
                        gateway_name=self.gateway_name
                    )
                elif self.td_api_setting['broker'] in self.ths_client_list:
                    position = PositionData(
                        symbol=str(ret["证券代码"]),
                        exchange=MARKET2VT[ret["交易市场"]] if ret["交易市场"] else Exchange.SSE,
                        direction=Direction.LONG,
                        volume=float(ret["当前持仓"]),
                        frozen=float(ret["当前持仓"] - ret["股份可用"]),
                        price=float(ret["参考成本价"]),
                        pnl=float(ret["参考盈亏"]),
                        yd_volume=float(ret["股份可用"]),
                        gateway_name=self.gateway_name
                    )
                self.on_position(position)
            self.write_log("账户持仓查询成功")
        except:
            self.write_log("账户持仓查询失败")

    def query_contract(self) -> None:
        try:
            self.write_log("行情接口开始获取合约信息 ...")
            sh_df = self.md_api.stocks(market=MARKET_SH)
            sh_stock_df = sh_df[sh_df['code'].str.contains("^((688)[\d]{3}|(60[\d]{4}))$")]
            sh_bond_df = sh_df[sh_df['code'].str.contains("^(110|113)[\d]{3}$")]
            sh_etf_df = sh_df[sh_df['code'].str.contains("^(58|51|56)[\d]{4}$")]

            sz_df = self.md_api.stocks(market=MARKET_SZ)
            sz_stock_df = sz_df[sz_df['code'].str.contains("^((002|000|300)[\d]{3})$")]
            sz_bond_df = sz_df[sz_df['code'].str.contains("^((127|128|123)[\d]{3})$")]
            sz_etf_df = sz_df[sz_df['code'].str.contains("^(15)[\d]{4}$")]

            exchange_list = [Exchange.SSE, Exchange.SZSE]
            for stock_df, exchange in zip([sh_stock_df, sz_stock_df], exchange_list):
                for row in stock_df.iterrows():
                    row = row[1]
                    contract: ContractData = ContractData(
                        symbol=row['code'],
                        exchange=exchange,
                        name=row["name"],
                        pricetick=0.01,
                        size=1,
                        min_volume=row['volunit'],
                        product=Product.EQUITY,
                        history_data=False,
                        gateway_name=self.gateway_name,
                    )
                    self.on_contract(contract)

            for bond_df, exchange in zip([sh_bond_df, sz_bond_df], exchange_list):
                for row in bond_df.iterrows():
                    row = row[1]
                    contract: ContractData = ContractData(
                        symbol=row['code'],
                        exchange=exchange,
                        name=row["name"],
                        pricetick=0.01,
                        size=1,
                        min_volume=row['volunit'],
                        product=Product.BOND,
                        history_data=False,
                        gateway_name=self.gateway_name,
                    )
                    self.on_contract(contract)

            for etf_df, exchange in zip([sh_etf_df, sz_etf_df], exchange_list):
                for row in etf_df.iterrows():
                    row = row[1]
                    contract: ContractData = ContractData(
                        symbol=row['code'],
                        exchange=exchange,
                        name=row["name"],
                        pricetick=0.01,
                        size=1,
                        min_volume=row['volunit'],
                        product=Product.ETF,
                        history_data=False,
                        gateway_name=self.gateway_name,
                    )
                    self.on_contract(contract)

        except:
            self.write_log("jotdx 行情接口获取合约信息出错")
