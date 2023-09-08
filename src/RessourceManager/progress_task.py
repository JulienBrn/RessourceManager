from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import concurrent, threading, time, asyncio, tqdm,time, functools


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
        # print("Checking for done", self.done())
        while not self.done():
            # print("Checking for done", self.done())
            res = self.get_progress()
            if not res is None:
                self._notify_progress(res[0], res[1])
            await asyncio.sleep(sleep_duration)
        return self.result()
        # print(self.result())


class ProgressExecutor(concurrent.futures.Executor):
    def submit(self, f, *args, **kwargs) -> ProgressTask:
        raise NotImplementedError("Abstract submit method")

def make_check_cancel(ev: threading.Event):
    def check_cancel():
        if not ev.is_set():
            ev.set()
            raise asyncio.CancelledError() from None
    return check_cancel

def make_set_cancel(ev):
    def set_cancel():
        ev.clear()
        print("waiting")
        ev.wait()
        print("waited")
    return set_cancel



class CustomUpdater:
    def __init__(self, *args, check_cancel, on_progress, **kwargs):
        self.n=0
        self.total=0
        self._child_init(check_cancel, on_progress)
        # super().__init__(*args, **kwargs, leave=False)
        # tqdm.tqdm._instances.remove(self)
        # self.set_description("child")
        self.display()
        

    def _child_init(self, check_cancel, on_progress):
        self.check_cancel = check_cancel
        self.on_progress = on_progress
        self.last_time = time.time()
        self.last_amount = 0

    def display(self, *args, **kwargs):
        # super().update(*args, **kwargs)
        # now = time.time()
        # if self.last_time - now > self.mininterval:
        self.check_cancel()
        # print("TOTAL", self.total)
        self.on_progress(self.n, self.total)
        # self.last_time = now
    # def update(self, n=1):
    #     # print("Updating")
    #     super().update(n)
        # print("Updated")


    # def refresh(self, *args, **kwargs):
    #     # print("refreshing")
    #     # print("again")
    #     # kwargs.update(nolock=True)
    #     # print(kwargs)
    #     super().refresh(*args, **kwargs)
        # print("refreshed")

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
    
    # def close(self): pass

def make_f(f, *args, cancel_ev, on_progress, init_progress, **kwargs):
    # print("make f")
    check_cancel = make_check_cancel(cancel_ev)
    on_progress = on_thread_progress(*init_progress)
    updater = CustomUpdater(check_cancel=check_cancel, on_progress=on_progress)
    # print("calling f")
    return f(*args, check_cancel = check_cancel, progress = updater, **kwargs)

def on_thread_progress(ev, info):
    def on_progress(n, tot):
        ev.set()
        info["n"] = n 
        info["tot"] = tot
    return on_progress

def make_get_thread_progress(info, ev):
    def get_progress():
        if not ev.is_set():
            return None
        else:
            ev.clear()
            return info["n"], info["tot"]
    return get_progress
    

def update_tqdm(tqdm, n, tot):
    tqdm.n = n
    tqdm.total=tot
    tqdm.update()

# class InfoHolder:
#     def __init__(self, n, tot):
#         self.n = n
#         self.tot = tot

class ThreadPoolProgressExecutor(concurrent.futures.ThreadPoolExecutor, ProgressExecutor):
    def __init__(self, *args, tqdm=tqdm.tqdm, **kwargs):
        super().__init__(*args, **kwargs)
        self.tqdm = tqdm

    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressTask:
        continue_ev = threading.Event()
        continue_ev.set()
        progress_ev = threading.Event()
        progress_info = dict(n=0, tot=1)
            
        # print("Sunmitted")
        t = super().submit(make_f, f, *args, cancel_ev = continue_ev, on_progress=on_thread_progress, init_progress=(progress_ev, progress_info) , **kwargs)
        # print("Submitted")
        t.__class__ = ProgressTask
        t._child_init(make_set_cancel(continue_ev), make_get_thread_progress(progress_info, progress_ev))
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: (progress_bar.close()))
        # print("Returning")
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
        progress_info["tot"] = 1
            
        # print("Sunmitted")
        t = super().submit(make_f, f, *args, cancel_ev = continue_ev, on_progress=on_thread_progress, init_progress=(progress_ev, progress_info) , **kwargs)
        # print("Submitted")
        t.__class__ = ProgressTask
        t._child_init(make_set_cancel(continue_ev), make_get_thread_progress(progress_info, progress_ev))
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: progress_bar.close())
        # print("Returning")
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

class SyncProgressExecutor(ProgressExecutor):
    def __init__(self, *args, tqdm=tqdm.tqdm, **kwargs):
        super().__init__(*args, **kwargs)
        self.tqdm = tqdm

    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressTask:
        continue_ev = SyncEvent()
        continue_ev.set()
        progress_ev = SyncEvent()
        progress_info = dict(n=0, tot=1)
        res = []
        check_cancel = lambda: None
        on_progress= lambda n, tot:res[0]._notify_progress(n, tot)


        async def run_f():
            await asyncio.sleep(0.1)
            return f(*args, check_cancel=check_cancel, progress=CustomUpdater(check_cancel=check_cancel, on_progress=on_progress))
        t = asyncio.create_task(run_f())
        # t.__class__ = ProgressTask
        import types
        t._child_init = types.MethodType(ProgressTask._child_init, t)
        t.add_progress_callback = types.MethodType(ProgressTask.add_progress_callback, t)
        t.check_for_progress = types.MethodType(ProgressTask.check_for_progress, t)
        t._notify_progress = types.MethodType(ProgressTask._notify_progress, t)
        t._child_init(lambda:None, lambda: (progress_info["n"], progress_info["tot"]))
        
        res.append(t)
        if not self.tqdm is None:
            progress_bar = self.tqdm(*progress_init_args)
            t.add_progress_callback(lambda n, tot: update_tqdm(progress_bar, n, tot))
            t.add_done_callback(lambda r: (progress_bar.close()))
        # print("Returning")
        return t
    
    def __enter__(self, *args, **kwargs):pass

    def __exit__(self, *args, **kwargs):pass














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
    