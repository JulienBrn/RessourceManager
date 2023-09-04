import threading, time, concurrent, asyncio, types, functools, concurrent, multiprocessing, logging
from multiprocessing.managers import SyncManager
import beautifullogger

logger = logging.getLogger(__name__)
beautifullogger.setup()

s = SyncManager()

def make_cancellable_future(name, executor):
    def _make_cancellable_future(f):
        async def new_f(*args, **kwargs):
            ev = threading.Event() if isinstance(executor, concurrent.futures.ThreadPoolExecutor) else s.Event()
            r = asyncio.get_running_loop().run_in_executor(executor, functools.partial(f, *args, ev, name))
            def ncancel(r, cls):
                r._old_cancel()
                ev.set()
            r._old_cancel = r.cancel
            r.cancel = types.MethodType(ncancel, r)
            return await r
        return new_f
    return _make_cancellable_future



def test(n, cancel: threading.Event, executor):
    tot = 17
    factor = 10000000
    for i in range(factor*n):
        if i%factor ==0:
            print(i, n, executor)
            if cancel.is_set():
                return
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
    return tot


async def main(name, exc):
    t1 = asyncio.create_task(make_cancellable_future(name, exc)(test)(10))
    t2 = asyncio.create_task(make_cancellable_future(name, exc)(test)(20))
    t3 = asyncio.create_task(make_cancellable_future(name, exc)(test)(15))
    subproc = await asyncio.subprocess.create_subprocess_shell("echo  ; echo going to sleep; sleep 20s; echo ; echo slept")
    t4 = asyncio.create_task(subproc.wait())
    t1.add_done_callback(lambda x: print(f"Finished t1 with {x}"))
    t2.add_done_callback(lambda x: print(f"Finished t2 with {x}"))
    t3.add_done_callback(lambda x: print(f"Finished t3 with {x}"))
    t4.add_done_callback(lambda x: print(f"Finished t4 with {x}"))
    r1 = await t1
    t2.cancel()
    t4.cancel()
    subproc.kill()
    try:
        r2 = await t2
    except:
        r2 = "cancelled"
    r3 = await t3
    try:
        r4 = await t4
    except:
        r4 = "cancelled"
    return (r1, r2, r3, r4)

if __name__ == "__main__":
    times = {}
    with s as s:
        for name, exc in {"3process": concurrent.futures.ProcessPoolExecutor(3), "3threads": concurrent.futures.ThreadPoolExecutor(3), "1thread": concurrent.futures.ThreadPoolExecutor(1)}.items():
            logger.info(f"start with {exc}")
            start = time.time()
            r = asyncio.run(main(name, exc), debug=True)
            logger.info(f"end res = {r}")
            end = time.time()
            times[name] = end-start
            input()
    print(times)