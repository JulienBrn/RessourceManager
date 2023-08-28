from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools, pathlib, pickle, shutil

class Storage:
    def has(self, r): raise NotImplementedError
    def load(self, r): raise NotImplementedError
    def dump(self, r, val): raise NotImplementedError

class MemoryStorage(Storage): 
    def __init__(self):
        self.d = {}
    def has(self, r):
        return r.storage_id in self.d
    def load(self, r):
        return self.d[r.storage_id]
    def dump(self, r, val):
        self.d[r.storage_id] = val
    def __repr__(self):
        return f"MemoryStorage"
    
class PickledDiskStorage(Storage):
    def __init__(self, base_folder=".cache"):
        self.base_folder=pathlib.Path(base_folder)
    def get_path(self, r):
        path: pathlib.Path = self.base_folder / r.group_name / str(r.storage_id) / "content.pkl"
        
        return path
    def has(self, r):
        return self.get_path(r).exists()
    def load(self, r):
        return pickle.load(open(self.get_path(r), "rb"))
    def dump(self, r, val):
        from tblib import pickling_support
        pickling_support.install()
        temp_path = pathlib.Path(str(self.get_path(r))+".temp")
        temp_path.parent.mkdir(exist_ok=True, parents=True)
        pickle.dump(val, open(temp_path, "wb"))
        if temp_path.exists():
            shutil.move(str(temp_path), str(self.get_path(r)))
        else:
            raise Exception(f"No file created by pickle dump... Expecting {temp_path}")
        
    def __repr__(self):
        return f"PickledDiskStorage({self.base_folder})"

class ReadableWriter(Storage):
    def __init__(self, base_folder=".readablecache"):
        self.base_folder=pathlib.Path(base_folder)  
    def get_path(self, r) -> pathlib.Path:
        return self.base_folder / r.group_name / str(r.storage_id)
    def has(self, r):
        return self.get_path(r).exists() and len(list(self.get_path(r).glob("content.*")))> 0
    def load(self, r):
        raise NotImplementedError("Readable writer should not be used to read")
    def dump(self, r, val):
        intro_text = f"#Result for ressource {r.identifier}\n\n"
        if isinstance(val, pd.DataFrame):
            fpath = pathlib.Path(self.get_path(r) / "content.tsv")
            temp_path = pathlib.Path(self.get_path(r) / "temp_content.tsv")
            temp_path.parent.mkdir(exist_ok=True, parents=True)
            temp_path.open("w").write(intro_text)
            val.to_csv(open(temp_path, "a"), sep="\t")
        else:
            fpath = pathlib.Path(self.get_path(r) / "content.txt")
            temp_path = pathlib.Path(self.get_path(r) / "temp_content.txt")
            temp_path.parent.mkdir(exist_ok=True, parents=True)
            temp_path.open("w").write(intro_text)
            if isinstance(val, np.ndarray):
                np.savetxt(open(temp_path, "a"), val)
            else:
                open(temp_path, "a").write(str(val))
        if temp_path.exists():
            # shutil.move(str(temp_path), str(fpath))
            pass
        else:
            raise Exception(f"No file created readable writer... Expecting {temp_path}")
        
    def __repr__(self):
        return f"ReadableWriter({self.base_folder})"


memory_storage = MemoryStorage()
pickled_disk_storage = PickledDiskStorage()