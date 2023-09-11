from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import concurrent, threading, time, asyncio, tqdm,time, functools, signal


class ProgressTask(concurrent.futures.Future):
    set_cancel: Callable[[], None]
    progress_callbacks: Callable[[float, float], None]
    get_progress: Callable[[], Optional[Tuple[float, float]]]

    def _child_init(self, set_cancel, get_progress):
        self.set_cancel = set_cancel
        self.progress_callbacks = []
        self.get_progress = get_progress

    def cancel(self):
        super().cancel()
        self.set_cancel()

    def add_progress_callback(self, fn):
        self.progress_callbacks.append(fn)

    def remove_progress_callback(self, fn):
        self.progress_callbacks.remove(fn)


    def _notify_progress(self, n: float, total: float):
        for c in self.progress_callbacks:
            c(n, total)

    async def check_for_progress(self, sleep_duration=0.1):
        try:
            while not self.done():
                res = self.get_progress()
                if not res is None:
                    self._notify_progress(res[0], res[1])
                await asyncio.sleep(sleep_duration)
            return self.result()
        except asyncio.CancelledError:
            self.cancel()
            raise


class ProgressExecutor(concurrent.futures.Executor):
    def submit(self, f, *args, **kwargs) -> ProgressTask:
        raise NotImplementedError("Abstract submit method")



class CustomUpdater:
    def __init__(self, *args, check_cancel, on_progress, on_close=lambda:None, **kwargs):
        self.n=0
        self.total=0
        self.check_cancel = check_cancel
        self.on_progress = on_progress
        self.on_close = on_close
        self.last_time = time.time()
        self.last_amount = self.n
        
        
    def display(self, *args, **kwargs):
        self.check_cancel()
        self.on_progress(self.n, self.total)


    def update(self, amount):
        self.n+=amount
        if self.total <= 0 or (self.n - self.last_amount)/self.total > 0.005:
            now = time.time()
            if now - self.last_time > 0.1:
                self.display()
                self.last_time = now
                self.last_amount = self.n


    def __call__(self, iterable):
        self.iterable = iterable
        self.total = len(iterable)
        self.n = 0
        return self
    
    def __iter__(self):
        for obj in self.iterable:
            yield obj
            self.update(1)
    
    def close(self):
        self.display()
        self.on_close()

def make_f(f, *args, cancel_ev, progress_ev, progress_info, **kwargs):
    def check_cancel():
        if not cancel_ev.is_set():
            cancel_ev.set()
            raise asyncio.CancelledError() from None

    def on_progress(n, tot):
        progress_info["n"] = n 
        progress_info["tot"] = tot
        progress_ev.set()

    def on_close():
        while progress_ev.is_set():
            time.sleep(0.1)
    

    updater = CustomUpdater(check_cancel=check_cancel, on_progress=on_progress, on_close =on_close)
    res = f(*args, check_cancel = check_cancel, progress = updater, **kwargs)
    updater.close()
    return res


def make_get_thread_progress(info, ev):
    def get_progress():
        if not ev.is_set():
            return None
        else:
            ev.clear()
            return info["n"], info["tot"]
    return get_progress
    

def make_set_cancel(ev):
    def set_cancel():
        ev.clear()
        # ev.wait()
    return set_cancel

def update_tqdm(tqdm, n, tot):
    tqdm.n = n
    tqdm.total=tot
    tqdm.update(0)

class ThreadPoolProgressExecutor(concurrent.futures.ThreadPoolExecutor, ProgressExecutor):
    def __init__(self, *args, tqdm=tqdm.tqdm, **kwargs):
        super().__init__(*args, **kwargs)
        self.tqdm = tqdm

    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressTask:
        continue_ev = threading.Event()
        continue_ev.set()
        progress_ev = threading.Event()
        progress_info = dict(n=0, tot=0)
            
        t = super().submit(make_f, f, *args, cancel_ev = continue_ev, progress_ev=progress_ev, progress_info=progress_info , **kwargs)

        t.__class__ = ProgressTask
        t._child_init(make_set_cancel(continue_ev), make_get_thread_progress(progress_info, progress_ev))
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: (progress_bar.close()))

        return t





class ProcessPoolProgressExecutor(concurrent.futures.ProcessPoolExecutor, ProgressExecutor):
    def __init__(self, *args, tqdm=tqdm.tqdm, **kwargs):
        super().__init__(*args, **kwargs)
        self.tqdm = tqdm

    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressTask:
        continue_ev = self.manager.Event()
        continue_ev.set()
        progress_ev = self.manager.Event()
        progress_info = self.manager.dict()
        progress_info["n"] = 0
        progress_info["tot"] = 0
            
        t = super().submit(make_f, f, *args, cancel_ev = continue_ev, progress_ev=progress_ev, progress_info=progress_info , **kwargs)

        t.__class__ = ProgressTask
        t._child_init(make_set_cancel(continue_ev), make_get_thread_progress(progress_info, progress_ev))
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: progress_bar.close())

        return t
    
    def __enter__(self, *args, **kwargs):
        import multiprocessing
        super().__enter__(*args, **kwargs)
        self.manager = multiprocessing.Manager()

    def __exit__(self, *args, **kwargs):
        import multiprocessing
        super().__exit__(*args, **kwargs)
        self.manager.shutdown()

class SyncEvent:
    def __init__(self):
        self.ev = False
    def set(self):
        self.ev = True
    def is_set(self):
        return self.ev


class SyncProgressTask(ProgressTask):
    progress_callbacks: Callable[[float, float], None]


    def __init__(self, f, args, kwargs, executor, handlers=None):
        super().__init__()
        self.is_cancelled = False
        self.progress_callbacks = []
        self.args = args
        self.f = f
        self.kwargs = kwargs
        self.handlers = handlers
        self.executor = executor
        self.done_callbacks =[]
    def cancel(self):
        self.is_cancelled = True

    def add_progress_callback(self, fn):
        self.progress_callbacks.append(fn)

    def remove_progress_callback(self, fn):
        self.progress_callbacks.remove(fn)

    def add_done_callback(self, fn):
        self.done_callbacks.append(fn)


    def _notify_progress(self, n: float, total: float):
        for c in self.progress_callbacks:
            c(n, total)

    async def check_for_progress(self, sleep_duration=0.1):
        try:
            self.set_running_or_notify_cancel()
            if not self.is_cancelled:
                def mcheck_cancel():
                    if self.is_cancelled:
                        raise asyncio.CancelledError() from None
                progress = CustomUpdater(check_cancel=mcheck_cancel, on_progress=lambda n, tot: self._notify_progress(n, tot))
                if not self.handlers is None:
                    def myhandler(*args, **kwargs):
                        # print("HANDLER!!!")
                        # time.sleep(10)
                        self.handlers[0](*args, **kwargs)
                    signal.signal(signal.SIGINT, myhandler)
            
                    # input("Changed handlers")
                    try:
                        res = self.f(*self.args, check_cancel=mcheck_cancel, progress=progress, **self.kwargs)
                    except KeyboardInterrupt:
                        signal.signal(signal.SIGINT, self.handlers[1])
                        self.handlers[1](None, None)
                        self.executor.shutdown()
                        raise asyncio.CancelledError() from None
                    except:
                        signal.signal(signal.SIGINT, self.handlers[1])
                        raise
                else:
                    res = self.f(*self.args, check_cancel=mcheck_cancel, progress=progress, **self.kwargs)
                    return res
            else:
                raise asyncio.CancelledError() from None
        except BaseException as e:
            for c in self.done_callbacks:
                c(e)
            raise
        else:
            for c in self.done_callbacks:
                c(res)
            

class SyncProgressExecutor(ProgressExecutor):
    def __init__(self, *args, tqdm=tqdm.tqdm, **kwargs):
        super().__init__(*args, **kwargs)
        self.tqdm = tqdm
        self.shutdowned = False
        self.tasks=[]
        self.handlers =()
    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressTask:
        t= SyncProgressTask(f, args, kwargs, self, self.handlers)
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: (progress_bar.close()))
        self.tasks.append(t)
        return t
            # try:
            #     t.set_result(f(*args, check_cancel=check_cancel, progress=CustomUpdater(check_cancel=check_cancel, on_progress=on_progress)))
            # except KeyboardInterrupt:
            #     self.shutdown()
            # except Exception as e:
            #     t.set_exception(e)
        # async def run_f():
        #     await asyncio.sleep(0.1)
        #     return f(*args, check_cancel=check_cancel, progress=CustomUpdater(check_cancel=check_cancel, on_progress=on_progress))
        # t = asyncio.create_task(run_f())
        # t.__class__ = ProgressTask
        
        
        
        
        
        # print("Returning")
        return t
    
    def declare_handlers(self, default_handlers, loop_handler):
        self.handlers= (default_handlers, loop_handler)

    def shutdown(self, wait=True, *, cancel_futures=False):
        for t in self.tasks:
            t.cancel()
    def __enter__(self, *args, **kwargs):pass

    def __exit__(self, *args, **kwargs):
        self.shutdown()














# class ProgressTask(concurrent.futures.Future):
#     def __init__(self, *args, event_lock: Optional[threading.Event], progress_update: Callable[[], None], progress: tqdm.tqdm, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._child_init(event_lock, progress_update)

#     def _child_init(self, event_lock, progress_update, progress):
#         self.event_lock = event_lock
#         self._tqdm = progress
#         self.last_update = time.time()
#         self.progress_update = progress_update
#         if not self.event_lock is None:
#             self.event_lock.set()

#     def cancel(self):
#         if not self.event_lock is None:
#             self.event_lock.clear()
#             self.event_lock.wait()

#     @property
#     def tqdm(self):
#         now = time.time()
#         if now - self.last_update > 0.1:
#             self.progress_update()
#             self.last_update = now
#         return self._tqdm

# class ProgressExecutor(concurrent.futures.Executor):
#     def submit(self, f, *args, **kwargs) -> ProgressTask:
#         raise NotImplementedError("Abstract submit method")
    
# class ThreadPoolProgressExecutor(concurrent.futures.ThreadPoolExecutor, ProgressExecutor):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     def submit(self, f, *args, init_progress = tqdm.tqdm, **kwargs) -> ProgressTask:
#         ev = threading.Event()
#         p = init_progress()
#         t = super().submit(f, *args, cancel_ev = ev, progress=p, **kwargs)
#         t.__class__ = ProgressTask
#         t._child_init(ev, lambda: None, p)
#         return t
    



# class Updater(tqdm.tqdm):
#     def __init__(self, *args, shared_dict, **kwargs):
#         self.shared_dict = shared_dict
#         super().__init__(*args, **kwargs, disable=True)

#     def refresh(self, nolock=False, lock_args=None):
#         import multiprocessing
#         super().refresh(nolock, lock_args)
#         self.shared_dict["n"] = self.n
#         self.shared_dict["total"] = self.total

    

# def call_func_with_updater(f, *args, cancel_ev, init_progress: Callable[[], tqdm.tqdm], shared_dict, **kwargs):
#     p : tqdm.tqdm = init_progress(disable=True)
#     p.__class__ = Updater
#     p.shared_dict = shared_dict
#     return f(*args, cancel_ev = cancel_ev, progress = p, **kwargs)


# class ProcessPoolProgressExecutor(concurrent.futures.ProcessPoolExecutor, ProgressExecutor):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         from multiprocessing.managers import SyncManager
#         self.sync_manager = SyncManager()
#         self.sync_manager.__enter__()

#     def submit(self, f, *args, init_progress: Callable[[], tqdm.tqdm] = tqdm.tqdm, **kwargs) -> ProgressTask:
#         import multiprocessing
#         ev = self.sync_manager.Event()
#         shared_dict = self.sync_manager.dict(n=0, total=1)
#         tqdm = init_progress()
#         tqdm.set_description("parent")
#         t = super().submit(call_func_with_updater, f, *args, cancel_ev = ev, init_progress=init_progress, shared_dict = shared_dict, **kwargs)
#         t.__class__ = ProgressTask
#         def update():
#             tqdm.n = shared_dict["n"]
#             tqdm.total = shared_dict["total"]
#             tqdm.update(0)

#         t._child_init(ev, update, tqdm)
#         return t
    
#     def __del__(self):
#         self.sync_manager.shutdown()
    