from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools
from RessourceManager.lifting import Lifting
from RessourceManager.storage import Storage

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





class InputOptions:
    dependency: Literal("RessourceId") | Literal("Value") | Literal("Ignore")
    make_id: Callable[[Any], str]
    pass_as: Literal("Value") | Literal("Ressource") | Storage 
    action: Literal("Used") | Literal("Passed") | Literal("Other")
    exception: Literal("Propagate") | Literal("PassAsValue")
    lifting: Lifting

class ResultOptions:
    result_on: Literal("Return") | Storage
    make_id: Callable[[str, Dict[str, str], bool], str]

class ComputeOptions:
    progress: Optional(tqdm.tqdm)
    n_retries: int
    alternative_paths: List[Any] #Alternative computation paths dependant to what has already been computed
    

class RessourceStats:
    computed: pd.DataFrame #columns are computation_duration and date_of_computation
    loaded: pd.DataFrame #columns are storage, loading_duration and date_of_load
    write: pd.DataFrame #columns are storage, write_duration and date_of_write


class RessourceException(Exception):pass
class RessourceIDError(RessourceException):pass
class ComputationRessourceError(RessourceException):pass
class InputRessourceError(RessourceException):pass
class MissingRessourceError(RessourceException):pass
class LoadingRessourceError(RessourceException):pass

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
    f: Callable[..., Any]
    param_options: Dict[str, InputOptions]
    result_options: ResultOptions
    compute_options: ComputeOptions
    
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
                raise NotImplementedError(f"Lock technique is not yet implemented and therefore the input option combination: action={option.action}, pass_as={option.pass_as} is not possible")
                # for r in ressources:
                #     r.run(lock="all", force_compute=False)
                # values = ressources
            case "Used", storage:
                values = []
                for r in ressources:
                    values.append(r.write_on_storage(storage).get_location(storage))
            case "Passed", "Ressource":
                values = ressources
            case _:
                raise NotImplementedError(f"Unknown input option combination: action={option.action}, pass_as={option.pass_as}")
        return option.lifting.reconstruct(values)
    
    def compute_param_id(self, param: str, for_storage: bool) -> str:
        option = self.param_dict[param][1]
        param_value = self.param_dict[param][0]
        ressources = option.lifting.deconstruct(param_value)

        match option.dependency, for_storage:
            case "Ignore", _:
                raise Exception("Compute_param_id called on ignored parameter")
            case ("RessourceId", _) | ("Value", False):
                ids = []
                for r in ressources:
                    ids.append(r.get_id(for_storage))
            case ("Value", True):
                for r in ressources:
                    ids.append(r._get())
            case _:
                raise NotImplementedError(f"Unknown input option: dependency={option.dependency}")
            
        obj = option.lifting.reconstruct(ids)
        return option.make_id(obj)
    
    def compute_id(self, for_storage: bool):
        try:
            param_id_dict={k: self.compute_param_id(k, for_storage) for k,(v, opt) in self.param_dict.items() if not opt.dependency=="Ignore"}
            id = self.result_options.make_id(self.group_name, param_id_dict, for_storage)
        except Exception as e:
                self.log.append(dict(action="computing_id", result=e, time=None, computation_time=None, n_errors=1, n_warnings=0))
                raise RessourceIDError(f"Error while computing id for ressource of group {self.group_name}") from e
        self.log.append(dict(action="computing_id", result=id, time=None, computation_time=None, n_errors=0, n_warnings=0))
        return id

    @functools.cached_property()
    def identifier(self):
        return self.compute_id(for_storage=False)
    
    @functools.cached_property()
    def storage_id(self):
        return self.compute_id(for_storage=True)
    
    def get_id(self, for_storage: bool):
        if for_storage:
            return self.storage_id
        else:
            return self.identifier


    def _get(self):
        #First, check if ressource has already been computed
        for storage in self.readers:
            if storage.has(self):
                try:
                    res = storage.load(self)
                    self.log.append(dict(action="loading_ressource", storage=storage, time=None, computation_time=None, n_errors=0, n_warnings=0))
                    return res
                except Exception as e:
                    logger.exception("Impossible to read ressource to storage. Skipping storage", e)
        
        #Otherwise, compute the ressource
            #First, get the ressource parameters
        param_values = {k:self.compute_param_value(k) for k in self.param_dict.keys()}
            #Check if Exceptions should be propagated
        propagated_exceptions =[]
        for k in param_values.keys():
            if self.param_dict[k][1].exception=="Propagate" and isinstance(param_values[k], BaseException):
                try:
                    raise param_values[k]
                except (Exception, ExceptionGroup)  as e:
                    try:
                        raise InputRessourceError(f"Error in input {k} while computing ressource {self.identifier}") from e
                    except InputRessourceError as exc:
                        propagated_exceptions.append(exc)
        if len(propagated_exceptions) == 1:
            return propagated_exceptions[0]
        elif len(propagated_exceptions) > 1:
            return ExceptionGroup("Errors during computation of parameters for ressource {self.identifier}", propagated_exceptions)
        
            #Start the computation
        self.log.append(dict(action="computation_start"))
        try:
            res = self.f(**param_values)
        except (Exception, ExceptionGroup)  as e:
            try:
                raise ComputationRessourceError(f"Error in while computing ressource {self.identifier}") from e
            except ComputationRessourceError as excpt:
                res = excpt
        self.log.append(dict(action="computation_end"))
        if self.result_options.result_on != "Return":
            if not self.result_options.result_on.has(self):
                res = MissingRessourceError(f"Computation of ressource {self.identifier} should have stored result on storage {self.result_options.result_on}, but no ressource found")
            else:
                try:
                    res = self.result_options.result_on.load(self)
                except (Exception, ExceptionGroup)  as e:
                    res = LoadingRessourceError(f"Computation of ressource {self.identifier} should have stored result on storage {self.result_options.result_on}. Ressource found but load failed.")
        #Write the result to the relevant storages
        for storage in self.writers:
            if not storage.has(self):
                try:
                    storage.dump(self, res)
                    self.log.append(dict(action="writing ressource", storage=storage, result=res))
                    return res
                except Exception as e:
                    logger.exception("Impossible to write ressource to storage. Skipping storage", e)
        return res

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
        if s.has(self):
            pass
        elif self.is_stored(loadable=True):
            pass #basically get
        elif self.result_options.result_on == s:
            self._run()
        else:
            res = self.get()
            s.dump(self, res)
        return self


             























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
