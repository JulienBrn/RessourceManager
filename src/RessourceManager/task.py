from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools
from RessourceManager.lifting import EmbeddedTaskHandler
from RessourceManager.id_makers import unique_id, make_result_id
from RessourceManager.storage import Storage, memory_storage, pickled_disk_storage, JsonSerializable
import inspect, pathlib, traceback, datetime, threading, multiprocessing, graphviz
from RessourceManager.task_manager import TaskManager
from dataclasses import dataclass

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
    pass_as: Literal["value"] | Tuple[Literal["task"], Literal["computed", "nocompute", "may_compute"]] | Tuple[Literal["location"], Storage | List[Storage]] = "value"
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
    result_location: Literal["returned"] | Storage = "returned"
    progress: bool
    n_retries: int
    constraints: ComputationConstraints
    alternative_paths: List[Any] #Alternative computation paths dependant to what has already been computed
    metadata: Optional[Callable[[Task, Any], JsonSerializable]] = lambda t, v: {
        "task": {
            "ids": {
                "identifier": t.identifier,
                "storage_id": t.storage_id
            },
            "param_dict": {
                k:{
                    "value": str(p),
                    "type": type(p),
                    "task_ids": None if not isinstance(p, Task) else {"identifier":p.identifier, "storage_id": p.storage_id},
                    "options": o
                }
                for k, (p, o) in t.param_dict.items()
            }
        },
        "returned": {
            "type": type(v),
            "shape": v.shape if hasattr(v, "shape") else None,
            "len": len(v) if hasattr(v, "__len__") else None,
        }
    }
    
@dataclass 
class StorageOptions:
    """
        Describes where a task should be attempted to be read and where a task should be attempted to be written.
        If a task can be both read and written from a same storage, we call that storage a checkpoint.
    """
    writers: List[Storage]  = [memory_storage, pickled_disk_storage]
    readers: List[Storage] = [memory_storage, pickled_disk_storage]

ComputationResult = Tuple[Storage, str]


@dataclass
class Task:
    param_dict: Dict[str, Tuple[Any, TaskParamOptions]]
    log: logging.Logger
    history: pd.DataFrame = pd.DataFrame(columns=["date", "action", "result", "comment"])
    storage_opt: StorageOptions
    group: TaskGroup
    f: Callable[..., Any]
    compute_options: ComputeOptions
    used_by: List[Task] = []
    manager: TaskManager
    computation_status: Literal["running", "loading", "none"] = "none"

    @functools.cached_property
    def identifier(self): raise NotImplementedError
    
    @functools.cached_property
    def storage_id(self): raise NotImplementedError

    @functools.cached_property
    def short_name(self): raise NotImplementedError
    
    def write_on_storage(self, s: Storage, progress = tqdm.tqdm): raise NotImplementedError
    def result(self, exception: (Literal["raise", "return"] | List[Exception]) = "raise", progress=tqdm.tqdm): raise NotImplementedError
    def invalidate(self): raise NotImplementedError
    def add_downstream_task(self, tasks: Task | List[Task]): raise NotImplementedError
    def get_dependency_graph(self, which=Literal["upstream", "downstream", "both"]) -> graphviz.Digraph: raise NotImplementedError
    def get_history(self) -> pd.DataFrame: raise NotImplementedError
    def get_stats(self) -> pd.DataFrame: raise NotImplementedError
    def can_get_result_as_python_object(self) -> bool: raise NotImplementedError

    def _get(self, ) -> ComputationResult: raise NotImplementedError


class TaskManager:
    def __init__(self): raise NotImplementedError
    def declare(self, task: Task): raise NotImplementedError
    def invalidate(self, tasks: Task | List[Task]): raise NotImplementedError
    def get_dependency_graph(self, groups_only=False) -> Any: raise NotImplementedError
    def set_computation_engine(self, e: TaskComputationEngine): pass

class TaskComputationEngine:
    def write_on_storage(self, tasks: Task | List[Task], s: Storage, progress = tqdm.tqdm): raise NotImplementedError
    def result(self, tasks: Task | List[Task], exception: (Literal["raise", "return"] | List[Exception]) = "raise", progress=tqdm.tqdm) -> Any | List[Any]: raise NotImplementedError