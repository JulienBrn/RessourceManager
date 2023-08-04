from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools
import pandas as pd, numpy as np, pickle
from tblib import pickling_support
import logging, hashlib

logger = logging.getLogger(__name__)

class DelayedException(Exception):
    def __init__(self, s):
        super().__init__(s)

class Storage:
    pass

class MemoryStorage(Storage):
    name = "memory"
    def __init__(self):
        self.storage = {}

    def make_location(self, id):
        return id
    
    def dump(self, val, loc):
        self.storage[loc] = val

    def load(self, loc):
        return self.storage[loc]

    def has(self, loc):
        return loc in self.storage
    
class PickledMemoryStorage(Storage):
    
    name = "pickledmemory"
    def __init__(self):
        self.storage = {}
        pickling_support.install()
        
    def make_location(self, id):
        return id
    
    def dump(self, val, loc):
        pickling_support.install()
        self.storage[loc] = pickle.dumps(val)

    def load(self, loc):
        pickling_support.install()
        return pickle.loads(self.storage[loc])
    
    def has(self, loc):
        return loc in self.storage
    
class PickledDiskStorage(Storage):
    def __init__(self, directory):
        pickling_support.install()
        self.name = f"DictStorage({directory})"
        self.cache_dir = pathlib.Path(directory)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def make_location(self, id):
        return self.cache_dir / (hashlib.sha256(id.encode()).hexdigest() + ".pkl")
    
    def dump(self, val, loc):
        pickle.dump(val, open(loc, "wb"))

    def load(self, loc):
        return pickle.load(open(loc, "rb"))

    def has(self, loc):
        return loc.exists()
    
pickled_disk_storage = PickledDiskStorage(".pickled_cache")
pickled_memory_storage = PickledMemoryStorage()
raw_memory_storage = MemoryStorage()
pickling_support.install()
d = {}

def mhash(v: Any):
    if isinstance(v, Ressource):
        return v.id
    elif isinstance(v, list):
        return f'[{",".join([mhash(x) for x in v])}]'
    elif isinstance(v, dict):
        return f'dict({",".join([f"{mhash(k)}={mhash(val)}" for k,val in sorted(v.items())])})'
    elif isinstance(v, str):
        return v
    elif isinstance(v, int) or isinstance(v, float) or isinstance(v, np.int64):
        return str(v)
    else:
        logger.warning(f"warning hash for {v} of type {type(v)}")
        return str(v)

def ressource_from_id(id):
    if id in d:
        r = Ressource.__new__()
        r.id = id
        return r
    else:
        raise KeyError(f"No ressource with id {id} declared")
    
def get_ressource_param(r: Ressource, d: pd.Series):
    param_type = d["parameter"]
    if param_type == "value":
        return r.get()
    if param_type == "ressource":
        r.get() #without the exception raising
        return r
    elif isinstance(param_type, Storage):
        storages=[param_type]
    elif hasattr(param_type, "__getitem__") and hasattr(param_type, "__len__") and len(param_type) >0:
        storages = param_type
    else:
        raise RuntimeError(f"Unknown parameter {param_type}")
    
    #Attempting to find a storage where it is already stored
    found=False
    for storage in storages:
        loc = storage.make_location(r.id)
        if storage.has(loc):
            res = (storage, loc)
            found=True
            break
    #Trying to compute the ressource on the given storage
    if not found:
        for storage in storages:
            loc = storage.make_location(r.id)
            try:
                r.write(storage)
                found=True
            except:pass
            if storage.has(loc):
                res = (storage, loc)
                found=True
                break
    if not found:
        raise RuntimeError("Impossible to get ressource on desired storage")
    return res
    



        
class Ressource:
    def __init__(self, name, f, arg_dict, params_df, 
                 write_checkpoints=[raw_memory_storage, pickled_memory_storage, pickled_disk_storage], read_checkpoints=[raw_memory_storage, pickled_memory_storage, pickled_disk_storage]):
        
        self.id = str(name) + mhash({k:v for k,v in arg_dict.items() if not params_df.loc[k, "ignore"]})[len("dict"):]
        # self.id = str(name) + str(sorted({k:v for k,v in arg_dict.items()}))
        if not self.id in d:
            d[self.id] = [name, f, arg_dict, params_df, [(storage, storage.make_location(self.id)) for storage in write_checkpoints], [(storage, storage.make_location(self.id)) for storage in read_checkpoints]]

    def get(self):
        [name, f, arg_dict, params_df, write_checkpoints, read_checkpoints] = d[self.id]
        found = False
        for storage, loc in read_checkpoints:
            if storage.has(loc):
                try:
                    res = storage.load(loc)
                    found=True
                    break
                except BaseException as e:
                    logger.exception("Impossible to read ressource to storage. Skipping storage", e)
        
        if not found:
            try:
                ressource_args = {k:get_ressource_param(v, params_df.loc[k, :]) for k, v in arg_dict.items() if isinstance(v, Ressource)}
                non_ressource_args = {k:v for k, v in arg_dict.items() if not isinstance(v, Ressource)}
            except BaseException as e:
                try:
                        #Not very readable technique to append this exception in the exception stack.
                        #I'm very open to something better, but it is important we do not change the type of the exception
                    raise DelayedException(f"Error to compute parameters for ressource {self.id}") from e
                except DelayedException as de:
                    res=de
                # try:
                #     print("EXCEPTION", e, "Cause", e.__cause__)
                #     raise e from type(e.__cause__)(str(e.__cause__))
                # except Exception as tmp:
                #     res = tmp 
                # res = None
                # res =e
                # res = "excpt"
                pass
            else:
                try:
                    res = f(**non_ressource_args, **ressource_args)
                except BaseException as e:
                    try:
                        #Not very readable technique to append this exception in the exception stack.
                        #I'm very open to something better, but it is important we do not change the type of the exception
                        raise DelayedException(f"Error in {f.__name__}({dict(**non_ressource_args, **ressource_args)}) for ressource {self.id}") from e
                    except DelayedException as de:
                        res=de
                        # try:
                        #     raise e from de
                        # except BaseException as final:
                        #     res = final
            
            

            

        for storage, loc in write_checkpoints:
            if not storage.has(loc):
                try:
                    storage.dump(res, loc)
                except BaseException as e:
                    logger.exception("Impossible to dump ressource to storage. Skipping storage", e)

        if isinstance(res, BaseException):
            raise res
        return res
    
    def write(self, storage):
        loc = storage.make_location(self.id)
        if not storage.has(loc):
            storage.dump(self.get())


# 

# class MemoryRessourceManager:
#     def __init__(self):
#         self.d={}
#     def declare_ressource(self, name, f, arg_dict):
#         id = myhash(name, arg_dict)
#         if id in d:
#             pass
#         else:
#             d[id] = [name, f, arg_dict, None]
#         return Ressource(id)
    
#     def get(self, id):
#         if isinstance(id, Ressource):
#             id = id.id
#         if id in self.d:
#             if self.d[id][3] is None:
#                 self.d[id][3] = self.d[id][1](**self.d[id][2])
#             return self.d[id][3]



# def compute_id(name, arg_dict, params_info):
#     hash_transform_arg = {k:myhash(v) if }
        


class RessourceDeclarator:
    def __init__(self, f, name, params_df):
        self.f = f
        self.name = name
        self.params_df = params_df
        self.vectorize =[]
        # self.vectorize = params_df.loc[params_df["vectorize"]].index

    def declare(self, *args, **kwargs):
        s = inspect.signature(self.f).bind(*args, **kwargs)
        s.apply_defaults()
        arg_dict = s.arguments

        if len(self.vectorize) ==0:
            return Ressource(self.name, self.f, arg_dict, self.params_df)
        else:
            unvectorized = {k:v for k,v in arg_dict.items() if k not in self.vectorize}
            d = pd.DataFrame(columns=list(self.vectorize))
            for k in self.vectorize:
                d[k] = arg_dict[k]
            res = d.apply(lambda row: Ressource(self.name, self.f, dict(**dict(row), **unvectorized), self.params_df), axis=1)
        return res

    def vectorized(self, *args, excluded={}):
        if len(args) > 0:
            param_list = set(args)
            if not len(excluded) == 0:
                logger.warning("warning excluded ignored")
        else:
            param_list = set(self.params_df.index)
            param_list = param_list - set(excluded)
        self.vectorize = param_list
        return self




# def add_stupid_doc(keys):
#     def mfunc(f):
#         @functools.wraps(f)
#         def new_f(*args, **kwargs):
#             return f(*args, **kwargs)
#         new_f.__doc__= f"Arguments: "+ str({k:None for k in defaults.keys()})
#         return new_f
#     return mfunc



class RessourceDecorator:
    default_params = dict(ignore=False, dtype=object, parameter="value", lift = None)
    #parameter = value | disk_storage_location | ressource
    def __init__(self, name, auto_save_on = None, loaders = None, run_restrictions=None, group=None, run_output = "return", vectorize = False, unload = None):
        self.params = {".all": RessourceDecorator.default_params}
        self.name = name
        pass

    def param(self, arg_names=None, ignore=None, parameter=None) -> RessourceDecorator:
        kwargs = {k:v for k,v in locals().items() if k in RessourceDecorator.default_params and not v is None}
        if arg_names is not None:
            if isinstance(arg_names, str):
                arg_names = [arg_names]
            for i, arg_name in enumerate(arg_names):
                if not arg_name in self.params:
                    self.params[arg_name] = {}
                for p,v in  kwargs.items():
                    if hasattr(v, "__getitem__") and hasattr(v, "__len__") and len(v) == len(arg_names):
                        self.params[arg_name][p] = v[i]
                    else:
                        self.params[arg_name][p] = v
        else:
            self.params[".all"].update(kwargs)
        return self
    
    def return_value(self, run_output = "return", auto_save_on = None, loaders = None, group=None, unload = None, dependency = None, dtype = None) -> RessourceDecorator:
        return self
    
    def run(self, run_restrictions=None) -> RessourceDecorator :
        return self

    def __call__(self, f):
        #Filling self.params_df
        arg_names = set(inspect.signature(f).parameters.keys())
        # print(arg_names)
        
        params = {}
        # print(self.params)
        for k in arg_names:
            params[k] = self.params[".all"].copy()
            if k in self.params:
                params[k].update(self.params[k])

        params_df = pd.DataFrame(params).transpose()

        if not set(params_df.index)  == arg_names:
            raise NameError(f"Expecting {set(params_df.index)} parameters for function {f}. Got {arg_names}.")
        

        
        return RessourceDeclarator(f, self.name, params_df)