from RessourceManager.progress_task import *
import time, asyncio

def long_compute(n):
    tot = 17
    for i in range(int(n*25000000)):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
        # print(updater.n)
    return tot


def f(n, check_cancel, progress: CustomUpdater):
    progress.total = n
    for i in progress(range(n)):
        long_compute(0.1)
    return n

tqdm_with_desc = lambda x: tqdm.tqdm(desc=x)
tp = ThreadPoolProgressExecutor(2, tqdm = tqdm_with_desc)
pp = ProcessPoolProgressExecutor(2, tqdm = tqdm_with_desc)
se = SyncProgressExecutor(tqdm = tqdm_with_desc)

executor = pp

async def main():
    with executor:
        async with asyncio.TaskGroup() as tg:
            fut1 = executor.submit(f, 30, progress_init_args=("t1",))
            fut2 = executor.submit(f, 40, progress_init_args=("t2",))
            fut3 = executor.submit(f, 35, progress_init_args=("t3",))

            t1 = tg.create_task(fut1.check_for_progress())
            t2 = tg.create_task(fut2.check_for_progress())
            t3 = tg.create_task(fut3.check_for_progress())

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
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(f"Total time {end-start}s")
    # print("done")