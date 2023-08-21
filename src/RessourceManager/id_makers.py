from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools


def unique_id(v: Any):
    if isinstance(v, list):
            return f'[{",".join([unique_id(x) for x in v])}]'
    elif isinstance(v, dict):
            return f'dict({",".join([f"{unique_id(k)}={unique_id(val)}" for k,val in sorted(v.items())])})'
    elif isinstance(v, str):
            return v
    elif isinstance(v, bytes):
            return f"{v}: bytes"
    elif isinstance(v, int) or isinstance(v, float) or isinstance(v, np.int64):
            return f"{str(v)}: {type(v).__name__}"
    else:
            raise Exception(f"Impossible to make_id {v} of type {type(v)}")
   
def hashed_id(v: Any):
    s = unique_id(v)
    return hashlib.md5(s).digest()

def make_result_id(name, param_dict, for_storage):
    arg_list = [f"{k}={val}" for k,val in sorted(param_dict.items())]
    id=f"{name}({', '.join(arg_list)})"
    return id if not for_storage else hashlib.md5(id.encode()).digest()