import asyncio
import threading
import datetime

from asyncio import AbstractEventLoop
from copy import copy
from typing import Dict

from easytrader import remoteclient

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.constant import Exchange, Direction, Offset, Status
from vnpy.trader.database import DATETIME_TZ
from vnpy.trader.object import AccountData, PositionData, CancelRequest, OrderRequest, OrderData, TradeData

MARKET2VT: Dict[str, Exchange] = {
    "深圳": Exchange.SZSE,
    "上海": Exchange.SSE,
}


class TradeDataTD:

    def __init__(self, gateway: BaseGateway):
        self.gateway = gateway
        self.api = None
        self.api_setting = {}
        self.thread: threading.Thread = None
        self.loop: AbstractEventLoop = None

        self.non_ths_client_list = ['htzq_client', 'ht_client', "gj_client"]
        self.ths_client_list = ['universal_client']

        self.datetime_format = "%Y-%m-%d %H:%M:%S"

    def start_loop(self, loop):
        """
        轮询
        使用easytrader查询1s ?? 变化的 当日成交 信息, 用于on_trade, 更新策略中的pos信息
        easytrader 还可以查询当日委托, 可以对比出 未成交单
        """
        asyncio.set_event_loop(loop)
        try:
            self.gateway.write_log("交易线程中启动协程 loop ...")
            loop.run_forever()
        except BaseException as err:
            self.gateway.write_log("交易线程中启动协程 loop 出现问题!")
            self.gateway.write_log(err)

    def connect(self, setting):
        self.api_setting = setting
        if setting['broker'] in self.non_ths_client_list:
            self.api = remoteclient.use(**setting)
        elif setting['broker'] in self.ths_client_list:
            # 通用同花顺客户端
            self.api = remoteclient.use(**setting)
            # remoteclient 同花顺远程输入代码存在问题, 不存在下面的方法
            # self.api.enable_type_keys_for_editor()
            self.gateway.write_log("同花顺远程输入代码存在问题, 注意测试 buy/sell 功能")
        else:
            # 其他券商专用同花顺客户端
            # 其他券商专用同花顺客户端不支持自动登录，需要先手动登录。
            # 请手动打开并登录客户端后，运用connect函数连接客户端。
            self.gateway.write_log("多线程不支持其他券商专用同花顺客户端")
        try:
            self.api.prepare(**setting)
            self.gateway.write_log("交易服务器连接成功!")
            self.query_account()
            self.query_position()
        except Exception as e:
            self.gateway.write_log(f"交易服务器连接失败! {e}")

        try:
            self.loop = asyncio.new_event_loop()  # 在当前线程下创建时间循环，（未启用），在start_loop里面启动它
            self.thread = threading.Thread(target=self.start_loop, args=(self.loop,))  # 通过当前线程开启新的线程去启动事件循环
            self.gateway.write_log("启动交易线程...")
            self.thread.start()
        except BaseException as err:
            self.gateway.write_log("交易线程启动出现问题!")
            self.gateway.write_log(err)

    def query_account(self) -> None:
        """查询资金"""
        try:
            ret = self.api.balance
            if self.api_setting['broker'] in self.non_ths_client_list:
                account: AccountData = AccountData(
                    gateway_name=self.gateway.gateway_name,
                    accountid=self.api_setting['broker'],
                    balance=ret['总资产'],
                    frozen=ret['总资产'] - ret['可用金额']
                )
            elif self.api_setting['broker'] in self.ths_client_list:
                account: AccountData = AccountData(
                    gateway_name=self.gateway.gateway_name,
                    accountid=ret['资金账号'],
                    balance=ret['总资产'],
                    frozen=ret['总资产'] - ret['可用资金']
                )
            else:
                account: AccountData = AccountData(
                    gateway_name=self.gateway.gateway_name,
                    accountid=ret['资金账号'],
                    balance=ret['总资产'],
                    frozen=ret['总资产'] - ret['可用资金']
                )

            self.gateway.on_account(account)
            self.gateway.write_log("账户资金查询成功")

        except BaseException as err:
            self.gateway.write_log("账户资金查询失败")
            self.gateway.write_log(err)

    def query_position(self) -> None:
        """查询持仓"""
        try:
            ret_list = self.api.position
            for ret in ret_list:

                trade_market_key = "交易市场"
                if trade_market_key in ret:
                    if ("沪" in ret[trade_market_key]) or ("上海" in ret[trade_market_key]):
                        ret[trade_market_key] = "上海"
                    elif ("深" in ret[trade_market_key]) or ("深圳" in ret[trade_market_key]):
                        ret[trade_market_key] = "深圳"

                if self.api_setting['broker'] in self.non_ths_client_list:
                    position = PositionData(
                        symbol=str(ret["证券代码"]),
                        exchange=MARKET2VT[ret["交易市场"]] if ret["交易市场"] else Exchange.SSE,
                        direction=Direction.LONG,
                        volume=float(ret["股票余额"]),
                        frozen=float(ret["冻结数量"]),
                        price=float(ret["成本价"]),
                        pnl=float(ret["盈亏"]),
                        yd_volume=float(ret["可用余额"]),
                        gateway_name=self.gateway.gateway_name
                    )
                elif self.api_setting['broker'] in self.ths_client_list:
                    position = PositionData(
                        symbol=str(ret["证券代码"]),
                        exchange=MARKET2VT[ret["交易市场"]] if ret["交易市场"] else Exchange.SSE,
                        direction=Direction.LONG,
                        volume=float(ret["当前持仓"]),
                        frozen=float(ret["当前持仓"] - ret["股份可用"]),
                        price=float(ret["参考成本价"]),
                        pnl=float(ret["参考盈亏"]),
                        yd_volume=float(ret["股份可用"]),
                        gateway_name=self.gateway.gateway_name
                    )
                else:
                    position = PositionData(
                        symbol=str(ret["证券代码"]),
                        exchange=MARKET2VT[ret["交易市场"]] if ret["交易市场"] else Exchange.SSE,
                        direction=Direction.LONG,
                        volume=float(ret["当前持仓"]),
                        frozen=float(ret["当前持仓"] - ret["股份可用"]),
                        price=float(ret["参考成本价"]),
                        pnl=float(ret["参考盈亏"]),
                        yd_volume=float(ret["股份可用"]),
                        gateway_name=self.gateway.gateway_name
                    )

                self.gateway.on_position(position)
            self.gateway.write_log("账户持仓查询成功")
        except BaseException as err:
            self.gateway.write_log("账户持仓查询失败")
            self.gateway.write_log(err)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        order_id = None
        try:
            if req.offset == Offset.OPEN:

                ret = self.api.buy(security=req.symbol, price=round(req.price, 2), amount=req.volume)
                order_id = ret.get('entrust_no', "success")

                order = req.create_order_data(order_id, self.gateway.gateway_name)
                order.status = Status.SUBMITTING
                self.gateway.orders[order_id] = order
                self.gateway.on_order(copy(order))

            elif req.offset == Offset.CLOSE:

                ret = self.api.sell(security=req.symbol, price=round(req.price, 2), amount=req.volume)
                order_id = ret.get('entrust_no', "success")

                order = req.create_order_data(order_id, self.gateway.gateway_name)
                order.status = Status.SUBMITTING
                self.gateway.orders[order_id] = order
                self.gateway.on_order(copy(order))

            if order_id == "success":
                self.gateway.write_log("系统配置未设置为 返回成交回报, 将影响撤单操作")
                return None

        except BaseException as e:
            order_id = "xxxxxx" if order_id is None else order_id
            order = req.create_order_data(order_id, self.gateway.gateway_name)
            order.status = Status.REJECTED
            self.gateway.on_order(copy(order))

            msg: str = f"开仓委托失败，信息：{e}"
            self.gateway.write_log(msg)

        finally:
            # check today trades
            trade = self.get_order_traded_data(order)
            if trade is not None:
                self.gateway.on_trade(copy(trade))
                order.status = Status.ALLTRADED
                self.gateway.orders[order_id] = order
                self.gateway.on_order(copy(order))

            return order.vt_orderid

    def get_order_traded_data(self, order: OrderData) -> TradeData:
        try:
            for trade in self.api.today_trades:
                trade_datetime = datetime.datetime.strptime(
                    f"{datetime.datetime.now(DATETIME_TZ).date()} {trade['成交时间'] if trade['成交时间'] else '00:00:00'}",
                    self.datetime_format
                ).replace(tzinfo=DATETIME_TZ)

                if order.orderid == trade['合同编号']:
                    trade = TradeData(
                        symbol=order.symbol,
                        exchange=order.exchange,
                        orderid=order.orderid,
                        tradeid=trade['成交编号'],
                        direction=order.direction,
                        offset=order.offset,
                        price=float(trade['成交均价']),
                        volume=float(trade['成交数量']),
                        datetime=trade_datetime,
                        gateway_name=self.gateway.gateway_name,
                    )
                    return trade

        except BaseException as e:
            self.gateway.write_log(f"获取订单成交数据出错: {e}")

        finally:
            return None

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        # check today order
        for order in self.api.today_entrusts:
            if order['合同编号'] == req.orderid and order['委托状态'] == "未成交":
                r = self.api.cancel_entrust(req.orderid)
                if "成功" in r.get('message', ''):
                    self.gateway.write_log(r['message'])
                else:
                    self.gateway.write_log(f"[{req.orderid}]撤单失败")
            else:
                self.gateway.write_log(
                    f"[{req.orderid}]不满足撤单条件无法撤单 或 未在交易委托系统中生成订单无需撤单"
                )

    def close(self):
        if self.api is not None:
            self.api.exit()
            self.gateway.write_log("交易服务器断开连接")
