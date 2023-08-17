from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools

logger = logging.getLogger(__name__)

def unique_id(v: Any):
   if isinstance(v, list):
         return f'[{",".join([unique_id(x) for x in v])}]'
   elif isinstance(v, dict):
         return f'dict({",".join([f"{unique_id(k)}={unique_id(val)}" for k,val in sorted(v.items())])})'
   elif isinstance(v, str):
         return v
   elif isinstance(v, int) or isinstance(v, float) or isinstance(v, np.int64):
         return f"{str(v)}: {type(v)}"
   else:
         raise Exception(f"Impossible to hash {v} of type {type(v)}")


def lift(f: Callable[[RessourceData], Any], v: Any):
    if isinstance(v, list):
            return [f(x) if isinstance (x, RessourceData) else x for x in v]
    elif isinstance(v, dict):
            return {f(k) if isinstance (k, RessourceData) else k:f(x) if isinstance (x, RessourceData) else x for k,x in v.items()}
    else:
            raise Exception(f"Impossible to lift {v} of type {type(v)}")

































class Storage:
    pass

class OutStorage(Storage):
    pass

class InStorage(Storage):
    pass

class IOStorage(OutStorage, InStorage):
    pass

class Lifting:
    def deconstruct(self, obj) -> List[RessourceData]:pass
    def reconstruct(self, ressources: List[Any]) -> Any: pass

class InputOptions:
    dependency: Literal("RessourceId") | Literal("Value") | Literal("Ignore")
    make_id: Callable[[Any], str]
    pass_as: Literal("Value") | Literal("Ressource") | Storage 
    action: Literal("Used") | Literal("Passed") | Literal("Other")
    exception: Literal("Reraise") | Literal("PassAsValue")
    lifting: Lifting

class ResultOptions:
    result_on: Literal("Return") | Storage

class ComputeOptions:
    progress: Optional(tqdm.tqdm)
    n_retries: int
    alternative_paths: List[Any] #Alternative computation paths dependant to what has already been computed
    

class RessourceStats:
    computed: pd.DataFrame #columns are computation_duration and date_of_computation
    loaded: pd.DataFrame #columns are storage, loading_duration and date_of_load
    write: pd.DataFrame #columns are storage, write_duration and date_of_write

class RessourceIDError(Exception):
     pass

class RessourceData:
    # Individual

        ## Individual non redundant information
    param_dict: Dict[str, Tuple[Any, InputOptions]]
    log: Any

        ## Kept for efficiency
    depends_on: Set[RessourceData]
    is_used_by: Set[RessourceData]

    # Possibly shared

        ## Storage options
    readers: List[Storage]
    writers: List[Storage]

        ## Function Definition
    group_name: str
    f: Callable[[Any], Any]
    param_options: Dict[str, InputOptions]
    result_options: ResultOptions
    compute_options: ComputeOptions

    @property
    def ressource_params(self):
        return {k:v for k,v in self.param_dict.items() if not v[1].dependency in [None, "ValueCheckpoint"]}
    
    @property
    def value_params(self):
        return {k:v for k,v in self.param_dict.items() if  v[1].dependency in [None, "ValueCheckpoint"]}
    
    @functools.cached_property
    def id(self):
        try:
            arg_id_value_dict= {k:unique_id(v.result() if isinstance(v, RessourceData) else v) for k, (v, o) in self.value_params.items() if not o.ignore}
            arg_id_ressource_dict= {k:lift(lambda x: x.id, v) for k, (v, o) in self.ressource_params.items() if not o.ignore}
            arg_id_dict = dict(**arg_id_value_dict, arg_id_ressource_dict)
            arg_list = [f"{k}={val}" for k,val in sorted(arg_id_dict.items())]
            id=f"{self.group_name}({', '.join(arg_list)})"
        except Exception as e:
             self.log.append(dict(action="computing_id", result=e, time=None, computation_time=None, n_errors=1, n_warnings=0))
             raise RessourceIDError(f"Error while computing id for ressource of group {self.group_name}") from e
        self.log.append(dict(action="computing_id", result=id, time=None, computation_time=None, n_errors=0, n_warnings=0))
        return id
    
    def compute_param_value(self, param: str):
        option = self.param_dict[param][1]
        param_value = self.param_dict[param][0]
        ressources = option.lifting.deconstruct(param_value)

        match (option.action, option.pass_as):
            case "Used", "Value":
                values = []
                for r in ressources:
                    values.append(r._get())
            case "Used", "Ressource":
                for r in ressources:
                    r.run(lock="all", force=False)
                values = ressources
            case "Used", storage:
                values = []
                for r in ressources:
                    values.append(r.write_on_storage(storage).get_location(storage))
            case "Passed", "Ressource":
                values = ressources
            case _:
                raise NotImplementedError(f"Unknown input option combination: action={option.action}, pass_as={option.pass_as}")
        return option.lifting.reconstruct(values)
    
    def compute_param_id(self, param: str) -> str:
        option = self.param_dict[param][1]
        param_value = self.param_dict[param][0]
        ressources = option.lifting.deconstruct(param_value)

        match option.dependency:
            case "Ignore":
                raise Exception("Compute_param_id called on ignored parameter")
            case "RessourceId":
                ids = []
                for r in ressources:
                    ids.append(r.id)
            case "Value":
                for r in ressources:
                    ids.append(r._get())
            case _:
                raise NotImplementedError(f"Unknown input option: dependency={option.dependency}")
            
        obj = option.lifting.reconstruct(ids)
        return option.make_id(obj)

















    def _get(self):
        for storage in self.readers:
            if storage.has(self):
                try:
                    res = storage.load(self)
                    self.log.append(dict(action="loading_id", storage=storage, time=None, computation_time=None, n_errors=0, n_warnings=0))
                    found=True
                    break
                except Exception as e:
                    logger.exception("Impossible to read ressource to storage. Skipping storage", e)
        if not found:
            if len(self.compute_options.alternative_paths) == 0:
                try:
                    arg_value_dict= {k:v.result() if isinstance(v, RessourceData) else v for k, (v, o) in self.value_params.items()}
                    ###STOPPED HERE
                    arg_id_ressource_dict= {k:lift(lambda x: x.result(), v) for k, (v, o) in self.ressource_params.items()}
                    non_ressource_args = {k:v for k, v in arg_dict.items() if not isinstance(v, Ressource)}
                except Exception as e:
                     res = e
            else:
                 raise NotImplementedError("")

    def result(self):
        res = self._get()
        if isinstance(res, Exception):
             raise res
        else:
             return res
        
    def invalidate(self):
        pass

    def remove_from_storage(self, s: Storage):
        pass

    def write_on_storage(self, s: Storage):
        pass

    def is_on_storage(self, s: Storage):
        pass
             























        def get_id_from_ressource(r: RessourceData):
            if r.result_options.is_value_dependency:
                try:
                    id = r.result()
                except:
                    raise #TODO
            else:
                return r.id
        
        

        self.id = str(self.group_name) + mhash({k:v for k,v in arg_dict.items() if not params_df.loc[k, "ignore"]})[len("dict"):]

        



    

class RessourceDecorator:
    pass

class RessourceManager:
    ressources: Dict[str, RessourceData]
    
    def ressource(self) -> RessourceDecorator: pass
