from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools

class Storage:
    def has(self, r): raise NotImplementedError
    def load(self, r): raise NotImplementedError
    def dump(self, r, val): raise NotImplementedError

class MemoryStorage(Storage): 
    def __init__(self):
        self.d = {}
    def has(self, r):
        return r.identifier in self.d
    def load(self, r):
        return self.d[r.identifier]
    def dump(self, r, val):
        self.d[r.identifier] = val

memory_storage = MemoryStorage()
pickled_disk_storage = MemoryStorage()