from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools

logger = logging.getLogger(__name__)

np.dtypes.Int64DType

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
    elif isinstance(v, np.dtype):
           return f"{str(v)}: np.dtype"
    elif v is None:
           return "None: NoneType"
    elif isinstance(v, pd.DataFrame):
            logger.warning("Making uniqueids of dataframes is experimental")
            r = v.reset_index().to_dict('records')
            return f"pd.Dataframe({unique_id({'data': list(v.reset_index().to_dict('records')), 'columns': list(v.columns), 'dtypes': v.dtypes.to_dict(), 'index': v.index.name})})"
    elif isinstance(v, pd.Series):
            logger.warning("Making uniqueids of series is experimental")
            return f"pd.Series({unique_id({'data': v.to_dict(), 'dtypes': v.dtypes})})"
    else:
            raise Exception(f"Impossible to make_id {v} of type {type(v)}")
   
def hashed_id(v: Any):
    s = unique_id(v)
    return hashlib.md5(s).hexdigest()

def make_result_id(name, param_dict, for_storage):
    arg_list = [f"{k}={unique_id(val)}" for k,val in sorted(param_dict.items())]
    id=f"{name}({', '.join(arg_list)})"
    return id if not for_storage else hashlib.md5(id.encode()).hexdigest()