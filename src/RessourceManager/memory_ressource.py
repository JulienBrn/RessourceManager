from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools
import pandas as pd, numpy as np, pickle
from tblib import pickling_support
import logging

logger = logging.getLogger(__name__)

class DelayedException(Exception):
    def __init__(self, s):
        super().__init__(s)


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
        print(f"warning hash for {v} of type {type(v)}")
        return str(v)

def ressource_from_id(id):
    if id in d:
        r = Ressource.__new__()
        r.id = id
        return r
    else:
        raise KeyError(f"No ressource with id {id} declared")

class Ressource:
    def __init__(self, name, f, arg_dict, params_df):
        
        self.id = str(name) + mhash({k:v for k,v in arg_dict.items() if not params_df.loc[k, "ignore"]})[len("dict"):]
        # self.id = str(name) + str(sorted({k:v for k,v in arg_dict.items()}))
        if not self.id in d:
            d[self.id] = [name, f, arg_dict, params_df, None]

    def get(self):
        [name, f, arg_dict, params_df, val] = d[self.id]
        ressource_args = {k:v for k, v in arg_dict.items() if isinstance(v, Ressource)}
        non_ressource_args = {k:v for k, v in arg_dict.items() if not isinstance(v, Ressource)}
        if val is None:
            try:
                res = f(**non_ressource_args, **{k:v.get() for k,v in ressource_args.items()})
            except BaseException as e:
                try:
                    #Not very readable technique to append this exception in the exception stack.
                    #I'm very open to something better, but it is important we do not change the type of the exception
                    raise DelayedException(f"Error in {f.__name__}({arg_dict})") 
                except DelayedException as de:
                    try:
                        raise e from de
                    except BaseException as final:
                        res = final

            pickling_support.install()
            res = pickle.dumps(res)
            d[self.id][-1] = res

        res = pickle.loads(d[self.id][-1])
        if isinstance(res, BaseException):
            raise res
        return res


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
    default_params = dict(ignore=False, dtype=object, parameter="value", lift = False)
    #parameter = value | disk_storage_location 
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
        print(arg_names)
        
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