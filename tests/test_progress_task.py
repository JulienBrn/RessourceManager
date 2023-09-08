from RessourceManager.progress_task import *
import time, asyncio, signal

def long_compute(n):
    tot = 17
    for i in range(int(n*25000000)):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
    return tot


def f(n, check_cancel, progress: CustomUpdater):
    progress.total = n
    for i in progress(range(2*n)):
        if i %2 ==0:
            long_compute(0.1)
        else:
            time.sleep(0.1)
    return n


defaultsiginthandler = signal.getsignal(signal.SIGINT)

tqdm_with_desc = lambda x: tqdm.tqdm(desc=x)
tp = ThreadPoolProgressExecutor(tqdm = tqdm_with_desc)
pp = ProcessPoolProgressExecutor(tqdm = tqdm_with_desc)
se = SyncProgressExecutor(tqdm = tqdm_with_desc)

executor = pp

async def main():
    loopsiginthandler = signal.getsignal(signal.SIGINT)
    se.declare_handlers(defaultsiginthandler, loopsiginthandler)
    vals = [30, 40, 35, 60, 20, 50, 38, 27]*2
    try:
        with executor:
            async with asyncio.TaskGroup() as tg:
                futs = [executor.submit(f, val, progress_init_args=(f"t{i} (val={val})",)) for i, val in enumerate(vals)]
                tasks = [tg.create_task(fut.check_for_progress()) for fut in futs]
                # await asyncio.sleep(2)
                # tasks[0].cancel()
    except asyncio.CancelledError: pass
    finally:
        await asyncio.sleep(0.1)
        for i, (val,task) in enumerate(zip(vals, tasks)):
            print(f"Task {i} with val={val} has result {'cancelled' if task.cancelled() else task.result()}")
    
        

if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(f"Total time {end-start}s")