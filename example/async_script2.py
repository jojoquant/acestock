import asyncio
from functools import partial
from tzlocal import get_localzone

import pandas as pd
from jotdx.quotes import Quotes
from datetime import datetime


# client = Quotes.factory(market='std')
# params = {"symbol": "600032", "start": 0, "offset": 50}
# df = partial(client.transaction, **params)()
# print(1)
from vnpy.trader.constant import Exchange
from vnpy.trader.object import TickData, SubscribeRequest


def trans_tick_df_to_tick_data(tick_df, req:SubscribeRequest):

    last_price = tick_df['price'][0]
    last_volume = tick_df['vol'][0]

    # num 放到turnover, 因为在bargenerater里面,
    # turnover是累加计算的, open_interest 是不算累加的而取截面的
    last_num = tick_df['num'][0]

    # buyorsell, 0 buy, 1 sell
    # buyorsell = tick_df['buyorsell'][0]

    tz = get_localzone()
    tick_datetime = datetime.now(tz)

    tick = TickData(
        gateway_name="paper",
        symbol=req.symbol,
        exchange=req.exchange,
        datetime=tick_datetime,
        volume=last_volume,
        turnover=last_num,
        last_price=last_price,
    )

    return tick


async def func(req: SubscribeRequest):
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

    if start_datetime <= tick_datetime <= end_datetime:
        pass

    while True:
        print(f"{req.symbol} func...")
        df1 = await loop.run_in_executor(None, partial(client.transaction, **params))
        last_tick_df = last_tick_df.append(df1).drop_duplicates()
        if len(last_tick_df) != 1:
            last_tick_df = df1
            tick = trans_tick_df_to_tick_data(last_tick_df, req)
            print("推送df1 -> tick: ", tick)
        await asyncio.sleep(1.5)

        df2 = await loop.run_in_executor(None, partial(client.transaction, **params))
        last_tick_df = last_tick_df.append(df2).drop_duplicates()
        if len(last_tick_df) != 1:
            last_tick_df = df2
            tick = trans_tick_df_to_tick_data(last_tick_df, req)
            print("推送df2 -> tick: ", tick)
        await asyncio.sleep(1.5)


async def async_timer(second=10):
    while True:
        print(f"time sleep {second}s")
        await asyncio.sleep(second)


# symbol_list = ['128050', "110051", "128096", "113039"]
symbol_list = [
    SubscribeRequest('128050', Exchange.SZSE),
    SubscribeRequest("600032", Exchange.SSE)
]
# symbol_list = []
task_list = [func(s) for s in symbol_list] + [async_timer(10)]
# done, pending = asyncio.run(asyncio.wait(task_list, timeout=10))
done, pending = asyncio.run(asyncio.wait(task_list))

# task_list = [asyncio.create_task(func(s)) for s in symbol_list]
# loop = asyncio.get_event_loop()
# loop.run_forever()

# print(pending)
# print(done)
