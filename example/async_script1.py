import asyncio


async def func():
    print("func...")
    await asyncio.sleep(2)
    print("func finished")
    return "func return value"


async def main():
    print("main...")
    task_list = [
        asyncio.create_task(func()),   # py3.7 不支持 name 参数
        asyncio.create_task(func())
    ]

    done, pending = await asyncio.wait(task_list, timeout=10)
    print(done)
    print(pending)
    print("main finished")


asyncio.run(main())

