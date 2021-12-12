import datetime
import time

import pandas as pd

from jnpy.gateway.acestock.acestock.gateway import AcestockGateway
from vnpy.trader.constant import Interval, Exchange, Direction, OrderType,Offset
from vnpy.trader.database import DB_TZ
from vnpy.trader.engine import EventEngine
from vnpy.trader.object import HistoryRequest, OrderRequest
from vnpy.trader.utility import load_json
from tzlocal import get_localzone
from vnpy.trader.database import DATETIME_TZ

if __name__ == '__main__':

    trade_datetime = datetime.datetime.now(DATETIME_TZ)
    trade_datetime = datetime.datetime.strptime(
        f"{datetime.datetime.now(DATETIME_TZ).date()} 09:54:41",
        "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=DATETIME_TZ)
    acestock_gateway = AcestockGateway(EventEngine())
    # df = acestock_gateway.md_api.bars(symbol="600036", frequency=0, offset=900, start=800)
    # df['datetime'] = pd.to_datetime(df['datetime'])
    # df.set_index('datetime', inplace=True)
    # df = df.tz_localize(DB_TZ)

    td_api_settings = load_json("connect_acestock.json")
    acestock_gateway.connect(td_api_settings)

    # req = HistoryRequest(
    #     symbol="113027",
    #     exchange=Exchange.SSE,
    #     start=datetime.datetime(year=2021, month=9, day=12, tzinfo=DB_TZ),
    #     end=datetime.datetime(year=2021, month=11, day=1, tzinfo=DB_TZ),
    #     interval=Interval.MINUTE_5,
    # )
    # r = acestock_gateway.query_history(req)
    #
    # order = OrderRequest(
    #     "113624",
    #     Exchange.SSE,
    #     Direction.SHORT,
    #     OrderType.LIMIT,
    #     volume=10,
    #     price=110.0,
    #     offset=Offset.CLOSE
    # )
    # acestock_gateway.send_order(order)

    for i in range(1):
        r1 = acestock_gateway.td_api.today_trades
        print(r1)
        r2 = acestock_gateway.td_api.today_entrusts
        print(r2)
        # time.sleep(1.5)

    print(1)
