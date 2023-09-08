from RessourceManager.progress_task import *
import time, asyncio

def f(n, check_cancel, progress: tqdm.tqdm):
    # print("called")
    progress.total = n
    # progress.set_description("child")
    for i in progress(range(n)):
        # print(i)
        time.sleep(0.1)
        # progress.update(1)
        # check_cancel()
    # input("done")
    return n

tqdm_with_desc = lambda x: tqdm.tqdm(desc=x)
tp = ThreadPoolProgressExecutor(3, tqdm = tqdm_with_desc)
pp = ProcessPoolProgressExecutor(3, tqdm = tqdm_with_desc)
se = SyncProgressExecutor(tqdm = tqdm_with_desc)

executor = tp

async def main():
    with executor:
        async with asyncio.TaskGroup() as tg:
            fut1 = executor.submit(f, 30, progress_init_args=("t1",))
            fut2 = executor.submit(f, 40, progress_init_args=("t2",))
            fut3 = executor.submit(f, 35, progress_init_args=("t3",))
            # await asyncio.sleep(1)
            # fut1.cancel()
            t1 = tg.create_task(fut1.check_for_progress())
            t2 = tg.create_task(fut2.check_for_progress())
            t3 = tg.create_task(fut3.check_for_progress())
    # print("\r")
    try: 
        print(f"t1 = {t1.result()}")
        pass
    except asyncio.CancelledError:
        print("t1 Cancelled")
    try:
        print(f"t2 = {t2.result()}")
        pass
    except asyncio.CancelledError:
        print("t2 Cancelled")
    try:
        print(f"t3 = {t3.result()}")
        pass
    except asyncio.CancelledError:
        print("t3 Cancelled")
        

if __name__ == "__main__":
    asyncio.run(main())
    # print("done")