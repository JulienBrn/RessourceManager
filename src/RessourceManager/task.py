from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, contextlib, concurrent, asyncio
# from RessourceManager.lifting import EmbeddedTaskHandler
from RessourceManager.id_makers import unique_id, make_result_id
from RessourceManager.storage import Storage, memory_storage, pickled_disk_storage, return_storage
import inspect, pathlib, traceback, datetime, threading, multiprocessing, graphviz
# from RessourceManager.task_manager import TaskManager
from dataclasses import dataclass, field
from progress_executor import ProgressExecutor

logger = logging.getLogger(__name__)

# @dataclass
# class TaskGroup:
#     name: str

@dataclass
class TaskParamOptions:
    """
        Options for parameters of the task.
        - dependency: states how that the task identifier depends on the parameter. Note that two task with the same identifiers are considered the "same". 
          i.e. if one is computed, to is the other
          
          - 'ignore' means that this parameter is ignored. For example, if the "debug" parameter is ignored, 
            f(1, debug=True) is considered the same task as f(1, debug=False)
          - 'value' means that the task depends on the parameter value (usual case for non task parameters). 
            For task parameters, this means that f(g(1, 2)) and f(g(2,1)) are considered the same ressource if g(1,2) = g(2,1)
            The internal handling of this case is non-trivial and requires computing both g(1,2) to know the storage_id of f(g(1, 2))
            and thus to know whether it has already been computed.
            If the goal is only computation, one should use the "alternative paths" method. 
            This technique is used mainly for storage purposes, so that the task result in only stored once.

          - 'graph' is only valid if the parameter is a task. 
            In this case, f(g(1, 2)) and f(g(2,1)) are not considered the same ressource, even if g(1,2) = g(2,1): there is no check.
            This is the basic handling of ressources and enables one to know whether f(g(1, 2)) has already been computed without getting the result of g(1, 2) beforehand.
        - pass_as: states how the parameter should be passed. If the parameter is not a task, it should always be 'value'. 
          Otherwise, during the computation the parameter will either be passed as a Task or a location(s) on storage(s).
          - In the former case, one should specify whether that task parameter will be computed in order to help engines schedule tasks on processes/threads
          - In the latter case, a dictionary of {storage:location} is passed if more than one location is desired.

        - exception: whether an exception in the parameter should propagate or should be passed as argument for the task (allowing it to raise it and except it as it wishes)
        - embedded_task_retriever: this parameters solves the problem of passing a List/Dict/... of tasks as an argument 
          while still enabling the dependency graph between tasks to be computed correctly.
          embedded_task_retriever(param) should return all tasks included in param and a function that given a new list of same size, 
          returns the same object with the tasks replaced by the values of the list.

        Note that input parameters should have __repr__ defined
    """
    dependency: Literal["ignore", "value", "graph"]
    pass_as: Literal["value"] | Tuple[Literal["task"], List[Storage]] | Tuple[Literal["location"], Storage] = "value"
    exception: Literal["propagate", "exception_as_arg"] = "propagate"
    embedded_task_retriever: Callable[[Any], Tuple[List[Task], Callable[[List[Any]], Any]]] = \
        lambda obj: ([], lambda l:obj) if not isinstance(obj, Task) else ([obj], lambda l:l[0])


@dataclass
class ComputeOptions:
    """
        - The result_location attribute specifies whether the function returns the task results 
          or actually keeps it stored on some storage (may be necessary if it does not fit in memory).
          In the latter case, the function should expect an additional argument '_location' corresponding to the location at which the result is expected.
        - the progress attribute specifies whether the function logs progress. If so, the function should expect an additional argument '_progress'
          which will behave similarly to tqdm.

        - The alternative_paths attribute is made to dynamically use the non natural path to compute the task depending on what has already been computed.
        For example, clusters(data, n_groups=3) might be computed using merge_clusters(clusters(data, n_groups=7), n_groups=3) 
        if clusters(data, n_groups=7) has already been computed.

        This attribute may also be used to specify that a already computed result is actually better that the one required and that one should use it.

        Currently, this attribute is not implemented.


        Other attributes should be auto-descriptive
    """
    result_storage: Storage = return_storage
    progress: bool = False
    n_retries: int = 1
    executor: Literal["async", "demanded"] | ProgressExecutor = "demanded"
    alternative_paths: List[Any] = ()#Alternative computation paths dependant to what has already been computed
    
@dataclass 
class StorageOptions:
    """
        Describes where a task should be attempted to be read and where a task should be attempted to be written.
        If a task can be both read and written from a same storage, we call that storage a checkpoint.
    """
    checkpoints: List[Storage]  = field(default_factory=lambda: [memory_storage, pickled_disk_storage])
    additional: List[Storage] = field(default_factory=lambda: [])

@dataclass
class HistoryEntry:
    action: Literal["computing_identifier", "computing_storage_id", "computing_short_name", "calling_f", "running", "dumping", "loading", "retrieving_params"]
    qualifier: Optional[Literal["start", "end"]]
    info: Optional[Any] = None #usually what storage is used in dumping, loading ; what thread, process is used for computing, ... ; what param is retrieved
    result_info: Any = None
    comment: Optional[str] = None
    date: datetime.datetime = field(default_factory=datetime.datetime.now)

    
def historize(action = None, info = None, result_info_computer: Callable[[Any], Any] = lambda x: str(x)[0:50],  comment=None):
    def decorator(f):
        nonlocal action, info
        if action is None:
            action = f.__name__
        
        def impl(self, *args, **kwargs):
            nonlocal action, info
            if info is None:
                info = (args, kwargs)
            self.list_history.append(HistoryEntry(action,  "start", info, None, comment))
            try:
                r = f(self, *args, **kwargs)
                self.list_history.append(HistoryEntry(action,  "end", info, result_info_computer(r), comment))
                return r
            except Exception as e:
                self.list_history.append(HistoryEntry(action,  "end", info, e, comment))
                raise e
            
        async def impl_async(self, *args, **kwargs):
            nonlocal action, info
            if info is None:
                info = (args, kwargs)
            self.list_history.append(HistoryEntry(action,  "start", info, None, comment))
            try:
                r = await f(self, *args, **kwargs)
                self.list_history.append(HistoryEntry(action,  "end", info, result_info_computer(r), comment))
                return r
            except Exception as e:
                self.list_history.append(HistoryEntry(action,  "end", info, e, comment))
                raise e
        # print(f"adding history decoration for {f.__name__}. is coroutine: {inspect.iscoroutinefunction(f)}")
        return impl if not inspect.iscoroutinefunction(f) else impl_async
    return decorator                


def format_exception_dict(cls, format_string: str, excpts: Dict[str, BaseException]):
    def format_exception_dict_impl(excpts):
        if isinstance(excpts, Exception):
            return excpts, [""]
        excpts = {k:format_exception_dict_impl(d) for k, d in excpts.items()}
        excpts = {k:(r, nks) for k, (r, nks) in excpts.items() if not r is None}

        if len(excpts) == 0:
            return None, [""]
        elif len(excpts) == 1:
            (k, (r, nks)) = excpts.popitem()
            if r is None:
                return None
            elif isinstance(r, ExceptionGroup):
                return r, [f"{k}.{nk}" for nk in nks]
            else:
                return r, nks
        else:
            return ExceptionGroup(f"keys={[k for k, (r, nks) in excpts.items()]}", [r for k, (r, nks) in excpts.items()]), excpts.keys()
        
    e, ks = format_exception_dict_impl(excpts)
    if not e is None:
        res = cls(format_string.format(ks))
        try:
            raise res from e
        except Exception as e:
            return e
    else:
        return None
    
def add_exception_note(note: str):
    def decorator(f):
        def new_f(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                e.add_note(note)
                raise e
        async def new_f_async(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except Exception as e:
                e.add_note(note)
                raise e
        # print(f"adding note {note} decoration for {f.__name__}. is coroutine: {inspect.iscoroutinefunction(f)}")
        return new_f if not inspect.iscoroutinefunction(f) else new_f_async 
    return decorator 

computation_asyncio_lock = {}

@dataclass
class Task:
    class TaskExceptionValue(Exception):pass
    class PropagatedException(TaskExceptionValue): pass
    class ComputationException(TaskExceptionValue): pass
    class MissingResultError(Exception): pass
    class NoneReturn: pass
    
    @dataclass
    class ParamInfo:
        options: TaskParamOptions
        reconstruct: Callable[[Dict[str, Task]], Any]
        embedded_tasks: Dict[str, Task]

        initial_param: Any = None
        embedded_retriever: Callable[[Any], Tuple[Dict[str, Task], Callable[[Dict[str, Task]], Any]]] = None
        dynamic: bool = False

    class LoadingTaskError(Exception): pass
    class InputTaskError(Exception): pass
    class DumpingTaskError(Exception): pass
    

    param_dict: Dict[str, ParamInfo]
    log: logging.Logger
    list_history: List[HistoryEntry]
    storage_opt: StorageOptions
    func_id: str
    f: Callable[..., Any]
    compute_options: ComputeOptions
    used_by: List[Task] = field(default_factory=lambda:[])
    
    @functools.cached_property #For each parameter the list of tasks in it
    def task_dependencies(self) -> Dict[str, List[Task]]: 
        return {k:o.embedded_task_retriever(v)[0] for k,(v,o) in self.param_dict if not o.dependency == "ignore"}


    @functools.cached_property
    @historize("computing_identifier")
    def identifier(self): 
        identifier_dict = {k:param.reconstruct({t_name: t.identifier for t_name,t in param.embedded_tasks.items()}) for k, param in self.param_dict.items() if not param.options.dependency=="ignore"}
        return make_result_id(self.func_id, identifier_dict, False)
    
    @functools.cached_property
    @historize("computing_storage_id")
    def storage_id(self):
        storage_id_dict = {
            k:param.reconstruct({
                t_name: t.storage_id if param.options.dependency=="graph" else t.result(exception="return") 
                    for t_name,t in param.embedded_tasks.items()
            }) for k, param in self.param_dict.items() if not param.options.dependency=="ignore"}
        
        return make_result_id(self.func_id, storage_id_dict, True)



    @functools.cached_property
    @historize("computing_short_name")
    def short_name(self): 
        return self.identifier[0:50] + ('...)' if len(self.identifier) > 49 else '')
    
    def __repr__(self):
        return self.identifier
    
    def __str__(self):
        return self.short_name
    




    async def result(self, executor: ProgressExecutor, exception: (Literal["raise", "return"] | List[Exception]) = "raise"): 
        for storage in self.storage_opt.checkpoints:
            with storage.lock(self):
                if storage.has(self):
                    res = await storage.load(self)
                    if exception == "raise" and isinstance(res, Exception):
                        raise res
                    else:
                        return res
        with self.compute_options.result_storage.lock(self):
            await self._run(executor=executor)
            res = await self.compute_options.result_storage.load(self)
            if exception == "raise" and isinstance(res, Exception):
                raise res
            else:
                return res
            
    async def write_to_storage(self, storage, executor: ProgressExecutor): 
        if not storage.has(self):
            with self.compute_options.result_storage.lock(self):
                await self._run(executor=executor)
                await self.compute_options.result_storage.transfert(self, storage)
                
    async def invalidate(self): 
        async with asyncio.TaskGroup() as tg:
            for task in self.used_by: 
                tg.create_task(task.invalidate())
            tg.create_task(self._remove())

    def add_downstream_task(self, tasks: Task | List[Task]): raise NotImplementedError
    def get_dependency_graph(self, which=Literal["upstream", "downstream", "both"]) -> graphviz.Digraph: raise NotImplementedError
    def get_history(self) -> pd.DataFrame: 
        df = pd.DataFrame()
        df["date"] = [x.date for x in self.list_history]
        df["qualifier"] = [x.qualifier for x in self.list_history]
        df["action"] = [x.action for x in self.list_history]
        df["info"] = [x.info for x in self.list_history]
        df["result"] = [x.result_info for x in self.list_history]
        return df
    def get_stats(self) -> pd.DataFrame: raise NotImplementedError


    @historize("remove")
    async def _remove(self):
        for storage in self.storage_opt.checkpoints + [self.compute_options.result_storage]:
            storage.remove(self)
        if hasattr(self, "short_name"): del self.short_name
        if hasattr(self, "identifier"): del self.identifier
        if hasattr(self, "storage_id"): del self.storage_id

    @historize("fetch_param")
    async def _get_param(self, opt: TaskParamOptions, task, context_stack: contextlib.ExitStack, executor: ProgressExecutor):
        match opt.pass_as:
            case "value":
                return await task.result(exception="return", executor = executor)
            case ("location", storage):
                context_stack.enter_context(storage.lock(task))
                if not storage.has(task):
                    await task.write_to_storage(storage, executor = executor)
                if storage.is_exception(task) and opt.exception=="propagate":
                    #Perhaps remove the lock?
                    return storage.load(task)
                return storage.get_location(task)
            case ("task", storages):
                for storage in storages:
                    context_stack.enter_context(storage.lock(task))
                    if not storage.has(task):
                        await task.write_to_storage(storage, executor = executor)
                    if storage.is_exception(task) and opt.exception=="propagate":
                        #Perhaps remove the lock?
                        return storage.load(task)
                return task
        
    @historize("running")
    async def _run(self, executor) -> None: 
        """
            Runs the computation and stores it to the default storages. Should not be called 
            if already stored.
        """
        with self.compute_options.result_storage.lock(self):
            with contextlib.ExitStack() as param_context_stack:
                @historize("Fetching parameters")
                @add_exception_note(f"During get_params for task {self.short_name}")
                async def get_params(self):
                    async with asyncio.TaskGroup() as tg:
                        subtasks = {k : {t_name: tg.create_task(add_exception_note(f"In fetching param {k}.{t_name}")(self._get_param)(self.param_dict[k].options, t, param_context_stack, executor)) for t_name, t in self.param_dict[k].embedded_tasks.items()} for k in self.param_dict}
                    embedded_args = {k: {t_name: t.result() for t_name, t in embedded.items()} for k, embedded in subtasks.items()}
                    excpts = {}
                    for k, t_name in [(k, t_name) for k in embedded_args for t_name in embedded_args[k]]:
                        if self.param_dict[k].options.exception=="propagate" and isinstance(embedded_args[k][t_name], Task.TaskExceptionValue):
                            if k not in excpts:
                                excpts[k] = {}
                            excpts[k][t_name] = embedded_args[k][t_name]
                    return excpts, embedded_args
                excpts, embedded_args = await get_params(self)

                if len(excpts) > 0:
                    result = format_exception_dict(Task.PropagatedException, "Propagated exception from input {}", excpts)
                else:
                    new_params = {k:self.param_dict[k].reconstruct(d) for k,d in embedded_args.items()}
                    short_name = ...
                    async def compute():
                        if not self.storage_id in computation_asyncio_lock:
                            computation_asyncio_lock[self.storage_id] = asyncio.Lock()
                        # async with computation_asyncio_lock[self.storage_id]:
                        if True:
                            if self.compute_options.result_storage.has(self):
                                return
                            
                            @historize("Computing")
                            @add_exception_note(f"During computation for task {short_name}")
                            async def run_f(self: Task):
                                mexecutor = self.compute_options.executor if not self.compute_options.executor == "demanded" else executor
                                match mexecutor:
                                    case "async":
                                        return await self.f(**new_params)
                                    
                                    case  custom_executor:
                                        fut = custom_executor.submit(self.f, **new_params)
                                        fut.add_tqdm_callback(tqdm.tqdm, dict(desc = self.short_name))
                                        try :
                                            return await fut.check_for_progress()
                                        except asyncio.CancelledError:
                                            raise
                            try:
                                result = await run_f(self)
                            except asyncio.CancelledError:
                                # logger.warning(f"Result cancelled for task {self.storage_id}")
                                raise
                            except Exception as e:
                                try:
                                    raise Task.ComputationException(f"Error in computation of {short_name}") from e
                                except Task.ComputationException as err:
                                    result = err
                            return result
                    
                    result = await compute()

            if not result is None:
                await self.compute_options.result_storage.dump(self, result if not isinstance(result, Task.NoneReturn) else None)
            if not self.compute_options.result_storage.has(self):
                raise Task.MissingResultError(f"Expected storage {self.compute_options.result_storage} to have result for task {self.short_name} with storage_id {self.storage_id} but storage does not have it...")
                
            @historize("Autostore")
            @add_exception_note(f"During storing of task {self.short_name}")
            async def automatic_store(s):
                async with asyncio.TaskGroup() as tg:
                    subtasks = {storage : tg.create_task(self.compute_options.result_storage.transfert(self, storage)) for storage in  self.storage_opt.additional + self.storage_opt.checkpoints}
            await automatic_store(self)



        

