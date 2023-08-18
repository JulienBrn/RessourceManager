from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools

class Storage:
    pass

class MemoryStorage(Storage): pass

memory_storage = MemoryStorage()
pickled_disk_storage = MemoryStorage()