from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, NewType, ContextManager
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, pathlib, pickle, shutil, threading, psutil
from dataclasses import dataclass
from RessourceManager.task import Task

JsonSerializable = NewType("JsonSerializable", Any)

class Storage:
    """
        Abstract class for the storage of task results. 
        A storage is responsable for storing information about a task result and being able to retrieve it.
        Note that storages may remove values stored whenever they wish (except when a value is locked), for example to avoid running out of memory.

        Storing Location and storage_id
        ----------
            Where the task result is stored is up to the storage. 
            However, the storage_id attribute of a task should uniquely determine the result of has/load functions.
            It is thus highly advised to use it (and not much else) to create the location at which the result is stored.
            However, additional information may be used: for example in database storage, one may use additional fields specific to the task, **as long as** 
            the storage_id is used as primary key.

            Furthermore, a task groups name is uniquely defined by the storage_id and may be used additionally to the storage_id safely.

        Storage format
        ----------
            what information about the result value is stored and in which format is stored is up to the storage. 
            It is not necessary to have load(dump(val)) = val, as the storage can be used for other purposes (for example saving metadata).
            The property load(dump(val)) = val is only necessary for storages used as checkpoints (see options of a Task).

        Storage volatility
        ----------
            In order to handle all kinds of storages, especially storages in memory which may run out of space, 
            a storage may unstore the results of a task at any point, **except** when locked.

            Locking is used especially during computation to ensure that a result used for another task in not deleted before being passed to the other task.
            Note that locking may be hard to protect from users (for example file deletion)... 


        Methods
        ----------
            has(task) 
                returns whether a task has been stored in this storage

            load(task) 
                loads the stored result for the task

            dump(task, val) 
                stores in this storage val as the result for the task

            remove(task) 
                removes the stored value of a task

            get_location(task) 
                should return the location at which the task results will be/is stored.
                The actual value and type returned is storage dependant.

            lock(task) 
                Is used to force task results to be kept:
                A Storage may "unstore" a value at any point during execution (for example when memory is overused), except for the tasks that are locked.
                Note that explicit calls, such as remove, will still work.
    """
    
    def has(self, task: Task) -> bool: 
        """
            Parameters
            ----------
                task: Task
                    the task that we want to know if a result is stored
            Returns
            -------
                Whether the task result is stored
        """
        raise NotImplementedError
    
    def load(self, task: Task) -> Any: 
        """
            Parameters
            ----------
                task: Task
                    the task whose result information we want to load
            Returns
            -------
                The loaded value.
            Raises
            -------
                MissingTaskResult if the storage does not have the task
        """
        raise NotImplementedError
    def dump(self, task: Task, val: Any) -> NoReturn: 
        """
            Parameters
            ----------
                task: Task
                    the task whose result information we want to store
                val: Any
                    the computation result of the task
        """
        raise NotImplementedError
    def remove(self, task: Task) -> NoReturn: 
        """
            Explicitly removes the result information of a task. 
            Note that this method will remove the task result, even if the task is locked.
            No errors are given if the result information of a task is not stored.

            Parameters
            ----------
                task: Task
                    the task whose result information we want to remove
        """
        raise NotImplementedError
    def get_location(self, task: Task) -> Any: 
        """
            Parameters
            ----------
                task: Task
                    the task whose storage location we want to get
            Returns
            -------
                The location for this storage. This value is used for Task that take other tasks locations as inputs.
                Note that the task does not need to be computed to know its location.
        """
        raise NotImplementedError
    def lock(self, task: Task | List[Task]) -> ContextManager[None]: 
        """
            Parameters
            ----------
                task: Task | List[Task]
                    the task whose result information we want to lock (no automatic removal by the storage)
            Returns
            -------
                a context manager that locks the task from automatic removal
            Note
            -------
                The task does not need to be already computed in order to lock it, a common pattern in computation engines is the following:
                with storage.lock(task):
                    task.compute()
                    res = f(task) 
                where f is some function that uses the result information of a task
        """
        raise NotImplementedError

class _Lock:
    def __init__(self, tasks: List[Task], storage):
        self.tasks = tasks
        self.storage = storage
    def __enter__(self):
        pass
    def __exit__(self):
        self.storage.locks.remove(self)


class MemoryStorage(Storage): 
    """
        Memory storage of task results (memoization).
        The technique used by this storage is simply a dictionary with keys task.storage_id.

        The main parameters are about when to free up the memory.
    """
    values: Dict[str, Any]
    metadata: Dict[str, Any]
    timer: Optional[threading.Timer]
    check_after_dump: bool

    def __init__(self, min_available: float = 8, check_timer: Optional[float]= 10, check_after_dump: bool = True):
        """
            Parameters
            ----------
                min_available: float
                    going under min_available (in Gb) memory available triggers the unstoring of all unlocked tasks results
                check_timer: Optional[float]
                    if not None, creates a threading.Timer that checks the available memory each check_timer seconds
                check_after_dump: bool
                    if True, the available memory is checked after each dump
            Note
            -------
                To never free up space, simply set min_available to 0 and/or no checks
        """

        self.locks = {}
        self.values = {}
        self.min_available = min_available
        if not check_timer is None:
            self.timer = threading.Timer(check_timer, lambda: self._free_if_necessary())
            self.timer.start()
        else:
            self.timer = None
        self.check_after_dump = check_after_dump

    def has(self, task):
        return task.storage_id in self.values
    
    def load(self, task):
        return self.values[task.storage_id]
    
    def dump(self, task, val: Any):
        self.values[task.storage_id] = val
        if self.check_after_dump:
            self._free_if_necessary()

    def remove(self, task):
        if task.storage_id in self.d:
            del self.values[task.storage_id]

    def get_location(self, task: Task):
        return task.storage_id
        
    def __repr__(self):
        return f"MemoryStorage"
    
    def lock(self, task: Task | List[Task]) -> ContextManager[None]:
        task = [task] if not isinstance(task, List) else task
        lock = _Lock([t.storage_id for t in task])
        self.locks.add(lock)
        return lock
    
    def free_up_space(self):
        for id in self.values:
            if not id in {i for l in self.locks for i in l.tasks}:
                del self.values[id]

    def _free_if_necessary(self):
        mem = psutil.virtual_memory()
        if mem.available/10**9 < self.min_available: 
            self.free_up_space()


def shape_type_metadata(t: Task, val: Any):
    return {
        "shape": val.shape if hasattr(val, "shape") else None,
        "len": len(val) if hasattr(val, "__len__") else None,
        "type": type(val)
    }

class MemoryMetadataStorage(Storage): 
    """
        Similar to MemoryStorage without the parameters to handle when memory is freed but with a parameter to specify what metadata is stored 
        from the task and the computed value.
    """
    metadata: Dict[str, Any]
    f: Callable[[Task, Any], Any]

    def __init__(self, f: Callable[[Task, Any], Any] = shape_type_metadata):
        self.locks = {}
        self.metadata = {}
        self.f = f

    def has(self, task):
        return task.storage_id in self.metadata
    
    def load(self, task):
        return self.metadata[task.storage_id]
    
    def dump(self, task, val: Any):
        self.metadata[task.storage_id] = self.f(task, val)

    def remove(self, task):
        if task.storage_id in self.d:
            del self.metadata[task.storage_id]

    def __repr__(self):
        return f"MemoryMetadataStorage"
    
    def lock(self, task) -> ContextManager[None]:
        task = [task] if not isinstance(task, List) else task
        lock = _Lock([t.storage_id for t in task])
        self.locks.add(lock)
        return lock
    
class NoLock:
    def __enter__(self):pass
    def __exit__(self):pass
    
class PickledDiskStorage(Storage):
    """
        Local disk storage of task results (persistence).
        This storage uses pickle to load/save the values to file.

        A task result is saved in the file 'base_folder / task.group_name / task.storage_id / "content.pkl"'
        In order to avoid problems when the process is interupted in the middle of a write, the task is first stored at 'base_folder / task.group_name / task.storage_id / "temp_content.pkl"'
        and then moved to 'base_folder / task.group_name / task.storage_id / "content.pkl"'

        Task results are never implicitly removed.
    """

    def __init__(self, base_folder=".cache"):
        self.base_folder=pathlib.Path(base_folder)

    def get_location(self, task):
        path: pathlib.Path = self.base_folder / task.group_name / str(task.storage_id)
        return path
    
    def has(self, task):
        return (self.get_location(task) / "content.pkl").exists()
    
    def load(self, task):
        return pickle.load((self.get_location(task) / "content.pkl").open("rb"))
    
    def dump(self, task, val):
        from tblib import pickling_support
        pickling_support.install()
        temp_path = self.get_location(task) / "temp_content"
        temp_path.parent.mkdir(exist_ok=True, parents=True)
        pickle.dump(val, temp_path.open("wb"))
        if temp_path.exists():
            shutil.move(str(temp_path), str(self.get_location(task) / "content.pkl"))
        else:
            raise Exception(f"No file created by pickle dump... Expecting {temp_path}")
        
    def remove(self, task):
        if self.get_location(task).exists():
            shutil.rmtree(str(self.get_location(task)))

    def lock(self, task) -> ContextManager[None]:
        return NoLock()
        
    def __repr__(self):
        return f"PickledDiskStorage({self.base_folder})"
    


class ReadableDiskWriter(Storage):
    """
        Default implementation of a human readable storage.
        Details are not yet settled.
    """

    def __init__(self, base_folder=".readablecache"):
        self.base_folder=pathlib.Path(base_folder)  

    def get_location(self, task) -> pathlib.Path:
        return self.base_folder / task.group_name / str(task.storage_id)
    def has(self, task):
        return self.get_location(task).exists() and len(list(self.get_path(task).glob("content.*")))> 0
    def load(self, task):
        raise NotImplementedError("Readable writer should not be used to read")
    def dump(self, task, val):
        intro_text = f"#Result for task {task}\n\n"
        if isinstance(val, pd.DataFrame):
            fpath = pathlib.Path(self.get_location(task) / "content.tsv")
            temp_path = pathlib.Path(self.get_location(task) / "temp_content.tsv")
            temp_path.parent.mkdir(exist_ok=True, parents=True)
            temp_path.open("w").write(intro_text)
            val.to_csv(temp_path.open("a"), sep="\t")
        else:
            fpath = pathlib.Path(self.get_location(task) / "content.txt")
            temp_path = pathlib.Path(self.get_location(task) / "temp_content.txt")
            temp_path.parent.mkdir(exist_ok=True, parents=True)
            temp_path.open("w").write(intro_text)
            if isinstance(val, np.ndarray):
                np.savetxt(temp_path.open("a"), val)
            else:
                temp_path.open("a").write(str(val))
        if temp_path.exists():
            shutil.move(str(temp_path), str(fpath))
        else:
            raise Exception(f"No file created readable writer... Expecting {temp_path}")
        
    def remove(self, task):
        if self.get_location(task).exists():
            shutil.rmtree(str(self.get_location(task)))

    def lock(self, task) -> ContextManager[None]:
        return NoLock()
    
    def __repr__(self):
        return f"ReadableWriter({self.base_folder})"


memory_storage = MemoryStorage()
pickled_disk_storage = PickledDiskStorage()
default_human_writer = ReadableDiskWriter()