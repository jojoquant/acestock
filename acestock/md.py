import asyncio
import threading
import datetime
from functools import partial

import pandas as pd

from asyncio import AbstractEventLoop
from typing import List

from jotdx.consts import MARKET_SH, MARKET_SZ
from jotdx.quotes import Quotes

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.database import DATETIME_TZ
from vnpy.trader.object import ContractData, HistoryRequest, BarData, SubscribeRequest, TickData
from vnpy.trader.constant import Product, Exchange, Interval
from vnpy.trader.utility import get_file_path, load_pickle, save_pickle

Interval_to_frequency_dict = {
    Interval.MINUTE: 8,
    Interval.MINUTE_5: 0,
    Interval.MINUTE_15: 1,
    Interval.MINUTE_30: 2,
    Interval.HOUR: 3,
    Interval.DAILY: 4,
    Interval.WEEKLY: 5,
}


class MarketDataMD:

    def __init__(self, gateway: BaseGateway):
        self.gateway = gateway
        self.api = None
        self.api_subscribe_req_list = []
        self.thread: threading.Thread = None
        self.loop: AbstractEventLoop = None
        self.contracts_dict = {
            Product.EQUITY: dict(),
            Product.BOND: dict(),
            Product.ETF: dict(),
        }
        self.save_contracts_pkl_file_name = f"{self.gateway.gateway_name}_contracts.pkl"

    def start_loop(self, loop):
        """
        轮询, 使用jotdx查询3s变化的 price 和 vol 信息, 生成缺少bid和ask的tick
        """
        asyncio.set_event_loop(loop)
        try:
            self.gateway.write_log("行情线程中启动协程 loop ...")
            loop.run_forever()
        except BaseException as err:
            self.gateway.write_log("行情线程中启动协程 loop 出现问题!")
            self.gateway.write_log(err)

    def connect(self, bestip: bool):
        self.api = Quotes.factory(market='std', bestip=bestip, heartbeat=True, multithread=True)
        self.query_contract()

        try:
            self.loop = asyncio.new_event_loop()  # 在当前线程下创建时间循环，（未启用），在start_loop里面启动它
            self.thread = threading.Thread(target=self.start_loop, args=(self.loop,))  # 通过当前线程开启新的线程去启动事件循环
            self.gateway.write_log("启动行情线程...")
            self.thread.start()
        except BaseException as err:
            self.gateway.write_log("行情线程启动出现问题!")
            self.gateway.write_log(err)

    def trans_tick_df_to_tick_data(self, tick_df, req: SubscribeRequest) -> TickData:
        # buyorsell, 0 buy, 1 sell
        # buyorsell = tick_df['buyorsell'][0]

        if any(req.symbol.startswith(stock_code) for stock_code in ["688", "60", "002", "000", "300"]):
            last_price = tick_df['price'][0]
            name = self.contracts_dict[Product.EQUITY][req.vt_symbol].name
        elif any(req.symbol.startswith(bond_code) for bond_code in ["110", "113", "127", "128", "123"]):
            last_price = round(tick_df['price'][0] / 10, 2)
            name = self.contracts_dict[Product.BOND][req.vt_symbol].name
        elif any(req.symbol.startswith(etf_code) for etf_code in ["58", "51", "56", "15"]):
            last_price = round(tick_df['price'][0] / 10, 2)
            name = self.contracts_dict[Product.ETF][req.vt_symbol].name
        else:
            last_price = 0.0
            name = "未知"

        return TickData(
            gateway_name=self.gateway.gateway_name,
            name=name,
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.datetime.now(DATETIME_TZ),
            volume=tick_df['vol'][0],
            # num 放到turnover, 因为在bargenerater里面,
            # turnover是累加计算的, open_interest 是不算累加的而取截面的
            turnover=tick_df['num'][0],
            last_price=last_price,
        )

    async def query_tick(self, req: SubscribeRequest):

        client = Quotes.factory(market='std')
        loop = asyncio.get_event_loop()

        params = {"symbol": req.symbol, "start": 0, "offset": 1}
        last_tick_df = await loop.run_in_executor(None, partial(client.transaction, **params))

        tz = DATETIME_TZ
        tick_datetime = datetime.datetime.now(tz)

        am_start_datetime = datetime.datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=9, minute=30, second=0, microsecond=0, tzinfo=tz)
        am_end_datetime = datetime.datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=11, minute=30, second=0, microsecond=0, tzinfo=tz)

        pm_start_datetime = datetime.datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=13, minute=0, second=0, microsecond=0, tzinfo=tz)
        pm_end_datetime = datetime.datetime(
            year=tick_datetime.year, month=tick_datetime.month, day=tick_datetime.day,
            hour=15, minute=0, second=0, microsecond=0, tzinfo=tz)

        if not last_tick_df.empty:
            tick = self.trans_tick_df_to_tick_data(last_tick_df, req)
            self.gateway.on_tick(tick)

        while True:
            if (am_start_datetime <= tick_datetime <= am_end_datetime) \
                    or (pm_start_datetime <= tick_datetime <= pm_end_datetime):
                df1 = await loop.run_in_executor(None, partial(client.transaction, **params))
                last_tick_df = last_tick_df.append(df1).drop_duplicates()
                if len(last_tick_df) != 1:
                    last_tick_df = df1
                    tick = self.trans_tick_df_to_tick_data(last_tick_df, req)
                    self.gateway.on_tick(tick)
                await asyncio.sleep(1.5)

                df2 = await loop.run_in_executor(None, partial(client.transaction, **params))
                last_tick_df = last_tick_df.append(df2).drop_duplicates()
                if len(last_tick_df) != 1:
                    last_tick_df = df2
                    tick = self.trans_tick_df_to_tick_data(last_tick_df, req)
                    self.gateway.on_tick(tick)

                await asyncio.sleep(1.5)
                # 这里注意要更新时间
                tick_datetime = datetime.datetime.now(tz)
            else:
                # 起到 heartbeat 的作用
                _ = await loop.run_in_executor(None, partial(client.transaction, **params))

                await asyncio.sleep(3)
                tick_datetime = datetime.datetime.now(tz)

    @staticmethod
    def drop_unused_bond_df_row(df, unused_symbol_list):
        if unused_symbol_list:
            return df[~df['code'].isin(unused_symbol_list)]
        return df

    def query_contract(self) -> None:
        contract_pkl_file_path = get_file_path(self.save_contracts_pkl_file_name)

        if contract_pkl_file_path.exists():
            # 判断文件更新日期, 如果当前日期 == 更新日期, 原则上每天只更新一次
            # 读取本地缓存文件
            update_date = datetime.datetime.fromtimestamp(
                contract_pkl_file_path.stat().st_mtime).date()
            if update_date == datetime.date.today():
                self.gateway.write_log("行情接口开始加载本地合约信息 ...")
                self.contracts_dict = load_pickle(self.save_contracts_pkl_file_name)
                [[self.gateway.on_contract(contract) for contract in v.values()] for v in self.contracts_dict.values()]
                return

        try:
            self.gateway.write_log("行情接口开始获取合约信息 ...")
            sh_df = self.api.stocks(market=MARKET_SH)
            sh_stock_df = sh_df[sh_df['code'].str.contains("^((688)[\d]{3}|(60[\d]{4}))$")]
            sh_bond_df = sh_df[sh_df['code'].str.contains("^(110|113)[\d]{3}$")]
            sh_etf_df = sh_df[sh_df['code'].str.contains("^(58|51|56)[\d]{4}$")]

            sz_df = self.api.stocks(market=MARKET_SZ)
            sz_stock_df = sz_df[sz_df['code'].str.contains("^((002|000|300)[\d]{3})$")]
            sz_bond_df = sz_df[sz_df['code'].str.contains("^((127|128|123)[\d]{3})$")]
            sz_etf_df = sz_df[sz_df['code'].str.contains("^(15)[\d]{4}$")]

            sh_bond_df = self.drop_unused_bond_df_row(
                sh_bond_df,
                ["110801", "110802", "110804", "110807", "110808",
                 "110810", "110811", "110812", "110813",
                 "113633", "113634", "113635", "113636"]
            )
            # sz_bond_df = self.drop_unused_bond_df_row(sz_bond_df, ["110801", "110802"])

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
                        history_data=True,
                        gateway_name=self.gateway.gateway_name,
                    )
                    self.gateway.on_contract(contract)
                    self.contracts_dict[Product.EQUITY][contract.vt_symbol] = contract

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
                        history_data=True,
                        gateway_name=self.gateway.gateway_name,
                    )
                    self.gateway.on_contract(contract)
                    self.contracts_dict[Product.BOND][contract.vt_symbol] = contract

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
                        history_data=True,
                        gateway_name=self.gateway.gateway_name,
                    )
                    self.gateway.on_contract(contract)
                    self.contracts_dict[Product.ETF][contract.vt_symbol] = contract
            try:
                save_pickle(self.save_contracts_pkl_file_name, self.contracts_dict)
                self.gateway.write_log("本地保存合约信息成功!")
            except BaseException as err:
                self.gateway.write_log("本地保存合约信息失败!")
                self.gateway.write_log(err)

        except Exception as e:
            self.gateway.write_log(f"jotdx 行情接口获取合约信息出错: {e}")

    def query_history(self, req: HistoryRequest) -> List[BarData]:

        history = []

        if req.end is None:
            offset = datetime.datetime.now(tz=DATETIME_TZ) - req.start
            offset = offset.days * 3600 * 4 + offset.seconds  # 每天交易4小时， 乘4 而不是24
        else:
            offset = req.end - req.start
            offset = offset.days * 3600 * 4 + offset.seconds

        if req.interval == Interval.MINUTE:
            offset = offset / 60
        elif req.interval == Interval.MINUTE_5:
            offset = offset / 60 / 5
        elif req.interval == Interval.MINUTE_15:
            offset = offset / 60 / 15
        elif req.interval == Interval.MINUTE_30:
            offset = offset / 60 / 30
        elif req.interval == Interval.HOUR:
            offset = offset / 60 / 60
        elif req.interval == Interval.DAILY:
            offset = offset / 60 / 60 / 24
        elif req.interval == Interval.WEEKLY:
            offset = offset / 60 / 60 / 24 / 5

        offset_const = 800  # pytdx 单次查询数据数目最大上限
        try:
            if offset > offset_const:
                start = 0
                df = self.api.bars(
                    symbol=req.symbol,
                    frequency=Interval_to_frequency_dict[req.interval],
                    offset=offset_const,
                    start=start
                )
                while offset > offset_const:
                    start += offset_const
                    offset -= offset_const
                    offset_const_df = self.api.bars(
                        symbol=req.symbol,
                        frequency=Interval_to_frequency_dict[req.interval],
                        offset=offset_const,
                        start=start
                    )
                    df = offset_const_df.append(df)
                    if len(offset_const_df) < offset_const:
                        offset = 0

                if offset > 0:
                    start += offset_const
                    res_df = self.api.bars(
                        symbol=req.symbol,
                        frequency=Interval_to_frequency_dict[req.interval],
                        offset=offset,
                        start=start
                    )
                    if len(res_df) != 0:
                        df = res_df.append(df)

            else:
                df = self.api.bars(
                    symbol=req.symbol,
                    frequency=Interval_to_frequency_dict[req.interval],
                    offset=int(offset)
                )
        except Exception as e:
            self.gateway.write_log(f"数据获取失败 {req}")
            self.gateway.write_log(f"Exception : {e}")
            return []

        if df.empty:
            return []

        # 因为 req 的 start 和 end datetime 是带tzinfo的, 所以这里将datetime列进行添加tzinfo处理
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df = df.tz_localize(DATETIME_TZ)
        df.reset_index(inplace=True)

        df = df[(df['datetime'] >= req.start) & (df['datetime'] <= req.end + datetime.timedelta(days=1))]
        self.gateway.write_log(f"查询历史数据成功, {req.start} -> {req.end}, 共{len(df)}条数据, 开始转换数据...")

        for _, series in df.iterrows():
            history.append(
                BarData(
                    gateway_name=self.gateway.gateway_name,
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=series['datetime'],
                    interval=req.interval,
                    volume=series['vol'],
                    turnover=series['amount'],
                    open_interest=0.0,
                    open_price=series['open'],
                    high_price=series['high'],
                    low_price=series['low'],
                    close_price=series['close']
                )
            )

        return history

    def close(self):
        if self.api is not None:
            self.api.close()
            self.gateway.write_log("行情服务器断开连接")
