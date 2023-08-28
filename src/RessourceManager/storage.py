from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, pathlib, pickle, shutil

class Storage:
    """
        Abstract class for the storage of task results. 
        The following methods are required:
        - has(task) returns whether a task has been stored in this storage
        - load(task) loads the stored result for the task
        - dump(task, val) stores in this storage val as the result for the task
        - remove(task) removes the stored value of a task
        - get_location(task) should return the location at which the task results will be/is stored.
          The actual value and type returned is storage dependant.

        Storages usually need to use unique_ids for tasks, for example to generate unique paths.
        The string attribute storage_id of the task should be used for that purpose, 
        however, one may use the additional information of the ressource either 
        - to generate additional information
        - organize task results (for example adding metadata in a database)
        
        Note that storages may remove tasks results whenever they wish, for example to avoid running out of memory 
    """
    def has(self, task) -> bool: raise NotImplementedError
    def load(self, task) -> Any: raise NotImplementedError
    def dump(self, task, val) -> NoReturn: raise NotImplementedError
    def remove(self, task) -> NoReturn: raise NotImplementedError
    def get_location(self, task) -> Any: raise NotImplementedError

class MemoryStorage(Storage): 
    """
        Memory storage of task results (memoization).
        The technique used by this storage is simply a dictionary with keys task.storage_id.
    """
    def __init__(self):
        self.d = {}
    def has(self, task):
        return task.storage_id in self.d
    
    def load(self, task):
        return self.d[task.storage_id]
    
    def dump(self, task, val):
        self.d[task.storage_id] = val

    def remove(self, task):
        if task.storage_id in self.d:
            del self.d[task.storage_id]

    def __repr__(self):
        return f"MemoryStorage"
    
class PickledDiskStorage(Storage):
    """
        Local disk storage of task results (persistence) in the folder "base_folder" (defaults to '.cache').
        This storage uses pickle to load/save the values to file.

        A task result is saved in the file 'base_folder / task.group_name / str(task.storage_id) / "content.pkl"'
        In order to avoid problems when the process is interupted in the middle of a write, the task is first stored at 'base_folder / task.group_name / task.storage_id / "temp_content.pkl"'
        and then moved to 'base_folder / task.group_name / task.storage_id / "content.pkl"'
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
        
    def __repr__(self):
        return f"PickledDiskStorage({self.base_folder})"

class ReadableWriter(Storage):
    """
        Default implementation of a human readable storage. 
        However, it is very hard to design a practical readable storage such that load(dump(val)) = val,
        therefore, we do not implement load and this storage is designed to write only.
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

    def __repr__(self):
        return f"ReadableWriter({self.base_folder})"


memory_storage = MemoryStorage()
pickled_disk_storage = PickledDiskStorage()
default_human_writer = ReadableWriter()