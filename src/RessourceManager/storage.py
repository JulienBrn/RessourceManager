from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, NewType, ContextManager
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, pathlib, pickle, shutil, threading, psutil, asyncio
from dataclasses import dataclass

logger = logging.getLogger(__name__)

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from RessourceManager.task import Task

class Storage:
    class MissingTaskResult(Exception):pass

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
    
    async def load(self, task: Task) -> Any: 
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
    async def dump(self, task: Task, val: Any) -> NoReturn: 
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
    
    def is_locked(self, task) -> bool:
        raise NotImplementedError
    
    async def transfert(self, task: Task, other: Storage) -> NoReturn:
        raise NotImplementedError
    
    def is_exception(self, task: Task) -> bool:
        load = None
        async def stupid(task):
            nonlocal load
            load = await self.load(task)
            
        coro = stupid(task)
        try:
            coro.send(None)
        except StopIteration:
            pass
        return isinstance(load, BaseException)


class LockImplStorage(Storage):
    """
        Abstract storage class giving a good default implementation of lock and is_locked 
        by having a lock counter.
    """

    class _Lock:
        def __init__(self, tasks: List[str], storage, callback):
            self.tasks = tasks
            self.storage = storage
            self.callback = callback
        def __enter__(self):
            for id in self.tasks:
                if not id in self.storage.lock_counters:
                    self.storage.lock_counters[id] =0
                self.storage.lock_counters[id]+=1
        def __exit__(self,  exc_type, exc_value, exc_tb):
            for id in self.tasks:
                self.storage.lock_counters[id]-=1
                if self.storage.lock_counters[id] ==0:
                    self.callback(id)


    lock_counters: Dict[str, int]
    def __init__(self, callback = lambda x:None):
        super().__init__()
        self.lock_counters = {}
        self.callback = callback

    def is_locked(self, task):
        return task.storage_id in self.lock_counters and self.lock_counters[task.storage_id] >0

    def lock(self, task: Task | List[Task]) -> ContextManager[None]:
        task = [task] if not isinstance(task, List) else task
        lock = LockImplStorage._Lock([t.storage_id for t in task], self, self.callback)
        return lock
    
class DictMemoryStorage(LockImplStorage):
    """
        Basic in Memory Storage using a dictionary. There is no automatic freeing of memory done.

        Memory storage of task results (memoization).
        The technique used by this storage is simply a dictionary with keys task.storage_id.
    """

    values: Dict[str, Any]

    def __init__(self, callback= lambda x:None):
        super().__init__(callback)
        self.values = {}

    def has(self, task):
        return task.storage_id in self.values
    
    async def load(self, task):
        return self.values[task.storage_id]
    
    async def dump(self, task, val: Any):
        self.values[task.storage_id] = val

    def remove(self, task):
        if task.storage_id in self.values:
            del self.values[task.storage_id]

    def get_location(self, task: Task):
        return task.storage_id
    
    async def transfert(self, task: Task, other: Storage) -> None:
        # print(f"transfert called with storage {self} to storage {other}")
        val = await self.load(task)
        await other.dump(task, val)
        
    def __repr__(self):
        return f"DictMemoryStorage"
    
    def free_up_space(self):
        for id in self.values:
            if self.is_locked(id):
                del self.values[id]
    
class AbstractLocalDiskStorage(LockImplStorage):
    """
        Abstract Local disk storage of task results (persistence).

        A task result is saved in the file 'base_folder / task.group_name / task.storage_id / "{content_name}.*"'
        In order to avoid problems when the process is interupted in the middle of a write, the task is first stored at 'base_folder / task.group_name / task.storage_id / "temp_{content_name}.temp"'
        and then moved to 'base_folder / task.group_name / task.storage_id / "{content_name}.*"'

        Abstract Methods
        -------
            load_content: path: pathlib.Path -> Any
                returns the content

            dump_content: path: pathlib.Path, val: Any -> str
                writes val to the file given by path.
                returns the desired extension for the file

        Notes
        -------
            Task results are not implicitly removed by this storage.

            If one has multiple LocalDiskStorages with same base_folder,
            one should take care of having a different content_name for each (otherwise there may be file name overlap)

            The get_location method returns 'base_folder / task.group_name / task.storage_id / "{content_name}"' as the extension may be unknown.
            It is up to you to then add the extension when being passed its location
    """
    base_folder: pathlib.Path
    content_name: str

    class FileConflictError(Exception):pass

    def __init__(self, content_name: str, base_folder: pathlib.Path | str =".cache"):
        super().__init__()
        self.base_folder=pathlib.Path(base_folder)
        self.content_name = content_name

    def get_folder_location(self, task: Task) -> pathlib.Path:
        return self.base_folder / task.func_id / str(task.storage_id)
    

    def get_location(self, task: Task) -> str:
        return self.get_folder_location(task) / self.content_name
    
    def get_file_location(self, task: Task) -> Optional[pathlib.Path]:
        matching = list(self.get_folder_location(task).glob(f"{self.content_name}.*"))
        if matching ==[]:
            return None
        elif len(matching) ==1:
            return matching[0]
        else:
            raise AbstractLocalDiskStorage.FileConflictError(f"Several paths match the target location... List is\n:{matching}")

    def has(self, task: Task) -> bool:
        return self.get_file_location(task) != None
    
    async def load(self, task: Task) -> Any:
        loc = self.get_file_location(task)
        if loc is None:
            raise Storage.MissingTaskResult(f"Expecting files matching {self.get_location()}.*")
        return await asyncio.to_thread(self.load_content, loc)
    

    async def dump(self, task: Task, val: Any):
        temp_path = self.get_folder_location(task) / f"temp_{self.content_name}.temp"
        temp_path.parent.mkdir(exist_ok=True, parents=True)
        extension = await asyncio.to_thread(self.dump_content, temp_path, val)
        if temp_path.exists():
            self.remove(task) #because there might be an old file with different extension that might not be rewritten
            shutil.move(str(temp_path), str(self.get_folder_location(task) / f"{self.content_name}.{extension}"))
        else:
            raise Exception(f"No file created by dump content... Expecting {temp_path}")
        
    def remove(self, task: Task):
        loc = self.get_file_location(task) 
        if not loc is None:
            if loc.is_file():
                loc.unlink()
            else:
                shutil.rmtree(str(loc))

    def load_content(self, path) -> Any: raise NotImplementedError("The function load_content should be defined in non-abstract children of AbstractLocalFileStorage")
    def dump_content(self, path, val) -> str: raise NotImplementedError("The function dump_content should be defined in non-abstract children of AbstractLocalFileStorage")
    


class ReturnStorage(DictMemoryStorage):
    """
        Class is used to store temporary results of return values in memory. 
        Only results that are locked are kept, otherwise results are removed.
        Its main purpose is to be used by engines.
    """
    def __init__(self):
        super().__init__(callback = lambda id: self.values.pop(id) if id in self.values else None)

    
    async def dump(self, task, val: Any):
        if self.is_locked(task):
            await super().dump(task, val)
        
    def __repr__(self):
        return f"ReturnStorage"
    

class MemoryStorage(DictMemoryStorage): 
    """
        Implementation of DictMemoryStorage that automatically frees up memory.
    """
    timer: Optional[threading.Timer]
    check_before_dump: bool
    min_available: float

    def __init__(self, min_available: float = 8, check_timer: Optional[float]= 10, check_before_dump: bool = True):
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
        super().__init__()
        self.min_available = min_available
        if not check_timer is None:
            self.timer = threading.Timer(check_timer, lambda: self.free_if_necessary())
            self.timer.start()
        else:
            self.timer = None
        self.check_before_dump = check_before_dump
    

    def free_if_necessary(self):
        mem = psutil.virtual_memory()
        if mem.available/10**9 < self.min_available: 
            self.free_up_space()

    async def dump(self, task, val: Any):
        if self.check_before_dump:
            self.free_if_necessary()
        await super().dump(task, val)
        
    def __repr__(self):
        return f"MemoryStorage"

    
class PickledDiskStorage(AbstractLocalDiskStorage):
    """
        Local disk storage of task results (persistence).
        This storage uses pickle to load/save the values to file.
    """

    def __init__(self, base_folder=".cache"):
        super().__init__("pickled_data", base_folder)

    def load_content(self, path) -> Any: 
        with path.open("rb") as f:
            return pickle.load(f)

    def dump_content(self, path: pathlib.Path, val) -> str: 
        # print(f"dump called with storage {self} path = {path.absolute()} val={val}")
        from tblib import pickling_support
        pickling_support.install()
        with path.open("wb") as f:
            pickle.dump(val, f)
        # print(f"dump done with storage {self} path = {path.absolute()}")
        return "pkl"

    def __repr__(self):
        return f"PickledDiskStorage({self.base_folder})"
    
class JsonLocalFileKeyStorage(LockImplStorage): 
    """
        Stores information about a result in a json file key.
    """

    class Remove: pass
    filename: str | pathlib.Path
    base_folder: pathlib.Path
    keys: Any
    f: Callable[[Task, Any], Any]

    def __init__(self, filename: str, key: int | str | List[int | str], f: Callable[[Task, Any], Any] = lambda task, val: val, base_folder = ".cache"):
        super().__init__()
        self.filename = filename
        self.base_folder = pathlib.Path(base_folder)
        self.keys = key if isinstance(key, list) else [key]
        self.f = f
        if pathlib.Path(filename).suffix !=".json":
            logger.warning("Expecting json file...")
    
    def get_file_location(self, task: Task) -> pathlib.Path:
        return self.base_folder / task.func_id / str(task.storage_id) / self.filename
        
    def get_location(self, task: Task) -> Tuple[pathlib.Path, str]:
        return (self.get_file_location(task), self.key)
    
    def has(self, task: Task) -> bool:
        import json
        if self.get_file_location(task).exists():
            d = json.load(self.get_file_location(task).open("r"))
            for key in self.keys:
                try:
                    d = d[key]
                except Exception:
                    return False
            return True
        else:
            return False
    
    async def load(self, task: Task) -> Any:
        import json
        return json.load(self.get_file_location(task).open("r"))
    
    async def dump(self, task: Task, val: Any):
        import json
        if self.get_file_location(task).exists():
            d = json.load(self.get_file_location(task).open("r"))
        else:
            d={}
        temp_path = self.get_file_location(task).with_stem("temp"+self.get_file_location(task).stem).with_suffix(".temp")
        temp_path.parent.mkdir(exist_ok=True, parents=True)
        dtmp = d
        for key in self.keys[:-1]:
            if not key in dtmp:
                dtmp[key] = {}
            dtmp = dtmp[key]
        if not isinstance(val, JsonLocalFileKeyStorage.Remove):
            dtmp[self.keys[-1]] = self.f(task, val)
        else:
            del dtmp
        json.dump(d, temp_path.open("w"), indent=4, default=lambda x: x.__dict__ if hasattr(x, "__dict__") and not isinstance(x, Storage)  and not hasattr(x, "_run")else str(x))
        if temp_path.exists():
            shutil.move(str(temp_path), str(self.get_file_location(task)))
        else:
            raise Exception(f"No file created by dump content... Expecting {temp_path}")
        
    def remove(self, task: Task):
        self.dump(task, JsonLocalFileKeyStorage.Remove())

class ReadableDiskWriter(AbstractLocalDiskStorage):
    """
        Default implementation of a human readable storage.
        Details are not yet settled.
    """

    def __init__(self, base_folder=".cache"):
        super().__init__("readable_data", base_folder)

    def load_content(self, path: pathlib.Path) -> Any: 
        if path.suffix =="tsv":
            return pd.read_csv(path.open("r"), sep="\t")
        if path.suffix =="json":
            import json
            return json.load(path.open("r"))
        elif path.suffix == "txt":
            return str(path.open("r").read())
        raise ValueError("Unknown extension error")
    
    def dump_content(self, path: pathlib.Path, val) -> str: 
        if isinstance(val, np.ndarray) and len(val.shape) ==2:
            val = pd.DataFrame(val)
        if isinstance(val, np.ndarray) and len(val.shape) ==1:
            val = pd.Series(val)
        if isinstance(val, pd.DataFrame) or isinstance(val, pd.Series):
            val.to_csv(path.open("w"), sep="\t")
            return "tsv"
        elif isinstance(val, dict) or isinstance(val, list):
            try:
                import json
                json.dump(path.open("w"), indent=4)
                return "json"
            except Exception:
                pass

        path.open("w").write(str(val))
        return "txt"


    def __repr__(self):
        return f"ReadableWriter({self.base_folder})"

return_storage = ReturnStorage()
memory_storage = MemoryStorage()
pickled_disk_storage = PickledDiskStorage()
readable_human_writer = ReadableDiskWriter()



def shape_type_metadata(t: Task, val: Any):
    return {
        "shape": val.shape if hasattr(val, "shape") else None,
        "len": len(val) if hasattr(val, "__len__") else None,
        "type": str(type(val)),
        "val": str(val)[0:50]
    }

def task_info_metadata(t: Task, val: Any):
    return {
        "identifier": t.identifier,
        "storage_id": t.storage_id,
        "params": t.param_dict,
        "func": t.f
    }

class MemoryMetadataStorage(DictMemoryStorage): 
    """
        Similar to MemoryStorage without the parameters to handle when memory is freed but with a parameter to specify what metadata is stored 
        from the task and the computed value.
    """

    f: Callable[[Task, Any], Any]

    def __init__(self, f: Callable[[Task, Any], Any] = shape_type_metadata):
        self.f = f

    
    async def dump(self, task, val: Any):
        await super().dump(task, self.f(task, val))

    def __repr__(self):
        return f"MemoryMetadataStorage"





memory_metadata_storage = MemoryMetadataStorage()
result_metadata_storage = JsonLocalFileKeyStorage("metadata.json", "result", shape_type_metadata)
task_info_metadata_storage = JsonLocalFileKeyStorage("metadata.json", "info", task_info_metadata)

