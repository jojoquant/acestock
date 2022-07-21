import asyncio
import threading
import datetime
from functools import partial

import pandas as pd

from asyncio import AbstractEventLoop
from typing import List, Dict

from jotdx.consts import MARKET_SH, MARKET_SZ
from jotdx.hq import TdxHq_API

from joconst.maps import INTERVAL_TDX_MAP, JONPY_TDX_MARKET_MAP
from jotdx.utils.best_ip_async import select_best_ip_async
from tqdm import tqdm

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import ContractData, HistoryRequest, BarData, SubscribeRequest, TickData
from vnpy.trader.constant import Product, Exchange, Interval
from vnpy.trader.utility import get_file_path, load_pickle, save_pickle, save_json
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed


class MarketDataMD:

    def __init__(self, gateway: BaseGateway):
        self.gateway: BaseGateway = gateway
        self.datafeed: BaseDatafeed = get_datafeed()
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
        self.sh_index_daily_bar_df = None

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

    def connect(self, setting: Dict[str, str]):
        '''
        setting 因为在widget里面显示text兼容，只能用str类型
        main_engine.connect 时候先读取 widget 的 setting，然后保存，然后连接调用该处函数
        连接流程在上游代码，无法改变，所以需要在这里再次保存一下最优 ip 和 port
        filename = f"connect_{gateway_name.lower()}.json"
        '''

        if self.datafeed.__class__.__name__ == "JotdxDatafeed":
            self.datafeed.init()
            self.api = self.datafeed.std_api
            setting['last_best_ip'] = self.api.ip
            setting['last_best_port'] = str(self.api.port)
        else:
            if (setting["update_bestip"] == "y") \
                    or (setting['last_best_ip'].strip() == "") \
                    or (setting['last_best_port'].strip() == ""):
                ip_port_dict = select_best_ip_async(_type="stock")
                setting['last_best_ip'] = ip_port_dict['ip']
                setting['last_best_port'] = str(ip_port_dict['port'])

            self.api = TdxHq_API()
            self.api.connect(ip=setting['last_best_ip'], port=int(setting['last_best_port']))

        save_json(f"connect_{self.gateway.gateway_name.lower()}.json", setting)

        self.query_contract()

        try:
            self.loop = asyncio.new_event_loop()  # 在当前线程下创建时间循环，（未启用），在start_loop里面启动它
            self.thread = threading.Thread(target=self.start_loop, args=(self.loop,))  # 通过当前线程开启新的线程去启动事件循环
            self.gateway.write_log("启动行情线程...")
            self.thread.start()
        except BaseException as err:
            self.gateway.write_log("行情线程启动出现问题!")
            self.gateway.write_log(err)

    def get_stocks(self, market):
        counts = self.api.get_security_count(market=market)
        stocks = None

        for start in tqdm(range(0, counts, 1000)):
            result = self.api.get_security_list(market=market, start=start)
            stocks = (
                pd.concat([stocks, self.api.to_df(result)], ignore_index=True)
                if start > 1 else self.api.to_df(result)
            )

        return stocks

    def update_sh_index_daily_bar_df(self):

        start = 0
        offset = 700
        all_df = pd.DataFrame()

        while True:
            df5 = self.api.to_df(
                self.api.get_index_bars(
                    category=INTERVAL_TDX_MAP[Interval.DAILY],
                    market=MARKET_SH, code="000001", start=start, count=offset
                )
            )

            if df5.empty:
                break

            all_df = pd.concat([df5, all_df])
            start += offset

        self.sh_index_daily_bar_df = all_df

    def get_tick_data_name(self, req: SubscribeRequest):
        if any(req.symbol.startswith(stock_code) for stock_code in ["688", "60", "002", "000", "300"]):
            name = self.contracts_dict[Product.EQUITY][req.vt_symbol].name
        elif any(req.symbol.startswith(bond_code) for bond_code in ["110", "113", "127", "128", "123"]):
            name = self.contracts_dict[Product.BOND][req.vt_symbol].name
        elif any(req.symbol.startswith(etf_code) for etf_code in ["58", "51", "56", "15"]):
            name = self.contracts_dict[Product.ETF][req.vt_symbol].name
        else:
            name = "未知"
        return name

    async def query_tick(self, req: SubscribeRequest):

        # [[1, '600301']]
        params = [[JONPY_TDX_MARKET_MAP[req.exchange], req.symbol]]

        last_tick_data_list = await self.loop.run_in_executor(
            None, partial(self.api.get_security_tick_data, params)
        )
        last_tick_data: TickData = last_tick_data_list[0]

        now_datetime = datetime.datetime.now()

        am_start_datetime = datetime.datetime(
            year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
            hour=9, minute=30, second=0, microsecond=0)
        am_end_datetime = datetime.datetime(
            year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
            hour=11, minute=31, second=0, microsecond=0)

        pm_start_datetime = datetime.datetime(
            year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
            hour=13, minute=0, second=0, microsecond=0)
        pm_end_datetime = datetime.datetime(
            year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
            hour=15, minute=1, second=0, microsecond=0)

        # tick_data_name = self.get_tick_data_name(req)

        while True:
            if (am_start_datetime <= now_datetime <= am_end_datetime) \
                    or (pm_start_datetime <= now_datetime <= pm_end_datetime):
                tick_data_list = await self.loop.run_in_executor(
                    None, partial(self.api.get_security_tick_data, params)
                )
                tick_data: TickData = tick_data_list[0]
                tick_data.name = self.get_tick_data_name(req)
                if tick_data.server_time_str != last_tick_data.server_time_str:
                    last_tick_data = tick_data
                    self.gateway.on_tick(tick_data)
                await asyncio.sleep(1.5)

                ######################################
                # For test 实盘时注释掉下面tick_data推送
                # self.gateway.on_tick(tick_data)
                # print("[acestock]" , tick_data)
                ######################################

                # 这里注意要更新时间
                now_datetime = datetime.datetime.now()
            else:
                # 起到 heartbeat 的作用
                _ = await self.loop.run_in_executor(None, partial(self.api.get_security_tick_data, params))

                await asyncio.sleep(3)
                now_datetime = datetime.datetime.now()

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
            sh_df = self.get_stocks(market=MARKET_SH)
            sh_stock_df = sh_df[sh_df['code'].str.contains("^((688)[\d]{3}|(60[\d]{4}))$")]
            sh_bond_df = sh_df[sh_df['code'].str.contains("^(110|113)[\d]{3}$")]
            sh_etf_df = sh_df[sh_df['code'].str.contains("^(58|51|56)[\d]{4}$")]

            sz_df = self.get_stocks(market=MARKET_SZ)
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

            last_stock_contract: ContractData = None
            last_bond_contract: ContractData = None
            last_etf_contract: ContractData = None
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
            last_stock_contract = contract

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
            last_bond_contract = contract

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
            last_etf_contract = contract

            try:
                cond1 = last_stock_contract.gateway_name == self.gateway.default_name
                cond2 = last_bond_contract.gateway_name == self.gateway.default_name
                cond3 = last_etf_contract.gateway_name == self.gateway.default_name
                if cond1 and cond2 and cond3:
                    save_pickle(self.save_contracts_pkl_file_name, self.contracts_dict)
                    self.gateway.write_log("本地保存合约信息成功!")

            except BaseException as err:
                self.gateway.write_log("本地保存合约信息失败!")
                self.gateway.write_log(err)

        except Exception as e:
            self.gateway.write_log(f"jotdx 行情接口获取合约信息出错: {e}")

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        return self.api.query_bar_history(req=req)

    def close(self):
        if self.api is not None:
            self.api.close()
            self.gateway.write_log("行情服务器断开连接")
