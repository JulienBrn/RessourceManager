from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict

from RessourceManager.task import Task
from typing import List
from RessourceManager.storage import Storage
import pandas as pd, tqdm, numpy as np

class TaskManager:
    def __init__(self):
        self.d={}
    def declare(self, task: Task):
        if not task.identifier in self.d:
            self.d[task.identifier] = task
            dependencies: List[Task] = task.dependencies
            for t in dependencies:
                t.add_usage(task)
        return self.d[task.identifier]
    
    def write_on_storage(self, tasks: Task | List[Task], s: Storage, progress = tqdm.tqdm): raise NotImplementedError
    def result(self, tasks: Task | List[Task], exception: (Literal["raise", "return"] | List[Exception]) = "raise", progress=tqdm.tqdm): raise NotImplementedError
    def invalidate(self): raise NotImplementedError
    def add_usage(tasks: Task | List[Task]): raise NotImplementedError
    def get_dependancy_graph(which=Literal["upstream", "downstream", "both"]) -> Any: raise NotImplementedError

    
