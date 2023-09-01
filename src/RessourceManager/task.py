from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, contextlib
from RessourceManager.lifting import EmbeddedTaskHandler
from RessourceManager.id_makers import unique_id, make_result_id
from RessourceManager.storage import Storage, memory_storage, pickled_disk_storage, return_storage, exception_storage
import inspect, pathlib, traceback, datetime, threading, multiprocessing, graphviz
from RessourceManager.task_manager import TaskManager
from dataclasses import dataclass, field

@dataclass
class TaskGroup:
    name: str

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
    pass_as: Literal["value"] | Tuple[Literal["task"], List[Storage]] | Tuple[Literal["location"], List[Storage]] = "value"
    exception: Literal["propagate", "exception_as_arg"] = "propagate"
    embedded_task_retriever: Callable[[Any], Tuple[List[Task], Callable[[List[Any]], Any]]] = \
        lambda obj: ([], lambda l:obj) if not isinstance(obj, Task) else ([obj], lambda l:l[0])


@dataclass 
class ComputationConstraints:
    threads: Literal["declared_thread", "main_thread", "any", "new_thread"] | threading.Thread = "any"
    processes: Literal["declared_process", "main_process", "any", "new_process"] | multiprocessing.Process = "any"
    gpu_to_use: Optional[str] = None
    already_parrallelized: bool = False

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
    progress: bool
    n_retries: int = 1
    constraints: ComputationConstraints
    alternative_paths: List[Any] #Alternative computation paths dependant to what has already been computed
    
@dataclass 
class StorageOptions:
    """
        Describes where a task should be attempted to be read and where a task should be attempted to be written.
        If a task can be both read and written from a same storage, we call that storage a checkpoint.
    """
    checkpoints: List[Storage]  = [memory_storage, pickled_disk_storage]
    additional: List[Storage] = [memory_storage, pickled_disk_storage]

@dataclass
class HistoryEntry:
    action: Literal["computing_identifier", "computing_storage_id", "computing_short_name", "calling_f", "running", "dumping", "loading", "retrieving_params"]
    qualifier: Optional[Literal["start", "end"]]
    info: Optional[Any] = None #usually what storage is used in dumping, loading ; what thread, process is used for computing, ... ; what param is retrieved
    result_info: Any = None
    comment: Optional[str] = None
    date: datetime.datetime = field(default_factory=datetime.now)

    
def historize(action = None, info = None, result_info_computer: Callable[[Any], Any] = lambda x:None,  comment=None):
    def decorator(f):
        if action is None:
            action = f.__name__
        def impl(self, *args, **kwargs):
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
        return impl
    return decorator                

@dataclass
class Task:
    class LoadingTaskError(Exception): pass
    class InputTaskError(Exception): pass
    class DumpingTaskError(Exception): pass
    class MissingResultError(Exception): pass

    param_dict: Dict[str, Tuple[Any, TaskParamOptions]]
    log: logging.Logger
    list_history: List[HistoryEntry]
    storage_opt: StorageOptions
    func_id: str
    f: Callable[..., Any]
    compute_options: ComputeOptions
    used_by: List[Task] = []
    
    manager: TaskManager
    
    @functools.cached_property #For each parameter the list of tasks in it
    def task_dependencies(self) -> Dict[str, List[Task]]: 
        return {k:o.embedded_task_retriever(v)[0] for k,(v,o) in self.param_dict if not o.dependency == "ignore"}

    @functools.cached_property
    @historize("computing_identifier")
    def identifier(self): 
        def get_param_id(v, o: TaskParamOptions):
            (l, reconstruct) = o.embedded_task_retriever(v)
            return unique_id(reconstruct([task.identifier for task in l]))

        params_id = {k:get_param_id(v, o) for k,(v,o) in self.param_dict if not o.dependency == "ignore"}
        return make_result_id(self.group.name, params_id, False)
    
    @functools.cached_property
    @historize("computing_storage_id")
    def storage_id(self):
        def get_param_id(v, o: TaskParamOptions):
            (l, reconstruct) = o.embedded_task_retriever(v)
            match o.dependency:
                case "graph":
                    return unique_id(reconstruct([task.storage_id for task in l]))
                case "value":
                    return unique_id(reconstruct([task.result() for task in l]))
                case _: 
                    raise ValueError(f"Unknown dependency option {o.dependency}")
                
        params_id = {k:get_param_id(v, o) for k,(v,o) in self.param_dict if not o.dependency == "ignore"}
        return make_result_id(self.group.name, params_id, True)



    @functools.cached_property
    @historize("computing_short_name")
    def short_name(self): 
        self.identifier[0:50] + ('...)' if len(self.identifier) > 49 else '')
    
    def __repr__(self):
        return self.identifier
    
    def __str__(self):
        return self.short_name
    
    def write_on_storage(self, s: Storage, progress = tqdm.tqdm): raise NotImplementedError
    def result(self, exception: (Literal["raise", "return"] | List[Exception]) = "raise", progress=tqdm.tqdm): raise NotImplementedError
    def invalidate(self): raise NotImplementedError
    def add_downstream_task(self, tasks: Task | List[Task]): raise NotImplementedError
    def get_dependency_graph(self, which=Literal["upstream", "downstream", "both"]) -> graphviz.Digraph: raise NotImplementedError
    def get_history(self) -> pd.DataFrame: raise NotImplementedError
    def get_stats(self) -> pd.DataFrame: raise NotImplementedError



    @historize("get_param")
    def get_param(self, key, context_stack: Optional[contextlib.ExitStack]):
        v, o = self.param_dict[key]
        (l, reconstruct) = o.embedded_task_retriever(v)
        excpt = []
        match o.pass_as:
            case "value":
                param_value = [t.result(exception="return") for t in l]
                for v in param_value:
                    if isinstance(v, Exception):
                        excpt.append(v)
            case ("task", storages) | ("location", storages):
                if not context_stack is None:
                    for t in l:
                        for storage in storages:
                            context_stack.enter_context(storage.lock(t)) #To ensure the parameter does not disappear from the location....
                for t in l:
                    for storage in storages:
                        if not storage.has(t):
                            t.write_on_storage(storage)   
                    x = t.load(return_storage) if return_storage.has(t) else t.load(exception_storage)
                    if isinstance(x, Exception):
                        excpt.append(x)     
            case _: 
                raise ValueError(f"Unknown pass_as option {o.pass_as}")
        match o.exception:
            case "propagate":   
                if not excpt ==[]:
                    if len(l) == 1:
                        raise excpt[0]
                    else:
                        raise ExceptionGroup("Errors in deconstructed tasks of parameter", excpt)
            case "exception_as_arg": pass
            case _:
                raise ValueError(f"Unknown exception option {o.exception}")
            
        match o.pass_as:
            case "value":
                return reconstruct(param_value)
            case "task", _:
                return v
            case "location", storages:
                return reconstruct([[storage.get_location(t) for storage in storages] for t in l])
            
    @historize("loading")
    def load(self, storage: Optional[Storage | List[Storage]]=None):
        if storage is None:
            storage = self.storage_opt.checkpoints
        if isinstance(storage, list):
            excpts = {}
            for s in storage:
                with s.lock(self):
                    if s.has(self):
                        try:
                            return self.load(s)
                        except Exception as e:
                            excpts[storage] = e
            if excpts == {}:
                raise Task.LoadingTaskError(f"Impossible to load result for task {self}: task is not stored on any checkpoints")
            else:
                raise ExceptionGroup(f"Impossible to load result for task {self}. Task was stored on {excpts.keys()}, but all storages had loading errors", list(excpts.values()))    
        else:
            try:
                res = storage.load(self)
                return res
            except Exception as e:
                raise Task.LoadingTaskError(f"Impossible to load result for task {self} from storage {storage} where it is stored") from e

    @historize("computation_store")  
    def store(self, storage=None):
        if storage is None:
            storage = self.storage_opt.checkpoints + self.storage_opt.additional
        if isinstance(storage, list):
            excpts = {}
            for s in storage:
                try:
                    self.store(self, s)
                except Exception as e:
                    excpts[storage] = e
            if not excpts == {}:
                raise ExceptionGroup(f"Impossible to store task {self} to storages {excpts.keys()}", list(excpts.values()))
        else:
            try:
                self.compute_options.result_storage.transfer(self, storage)
            except Exception as e:
                raise Task.DumpingTaskError(f"Impossible to store {self} to storage {storage}") from e

    @historize("compute", info="None")
    def compute(self, args):
        self.f(**args)

    @historize("running")
    def run(self) -> NoReturn: 
        """
            If not already stored in one of the readers, 
            lauches computation by first retrieving the arguments, calling f, and then stores in all writers.
            
            It is up to the caller (usually an engine) to ensure (if desired) 
            that the dependencies are already computed on a storage and locking those results
        """
        if self.is_stored(): 
            return
        else:
            with contextlib.ExitStack() as context_stack:
                args ={}
                excpts = {}
                for k in self.param_dict:
                    try:
                        val = self.get_param(k, context_stack)
                        args[k] = val
                    except Exception as e:
                        try:
                            raise Task.InputTaskError(f"Error while computing input {k} of {self}") from e
                        except Exception as e2:
                            excpts[k] = e2
                if not excpts == {}:
                    raise ExceptionGroup(f"Error while in inputs for task {self}. Errored arguments keys are {excpts.keys()}")
                try:
                    with self.compute_options.result_storage.lock(self):
                        self.compute(**args)
                        if not self.compute_options.result_storage.has(self):
                            raise Task.MissingResultError(f"Result was for task {self} was expected on storage {self.compute_options.result_storage}")
                        self.store()
                except * Task.DumpingTaskError as excpts:
                    for e in excpts.exceptions:
                        self.log.exception("DumpingError", exc_info=e)
                except * BaseException as e:
                    args_str = ",".join([f"{k}:{v[:10]}" for k,v in args.items()])
                    operation = f"{self.func_id}({args_str})"
                    raise Task.ComputeTaskError(f"Error while computing task {self} (operation was {operation})") from e

                
                    




        

