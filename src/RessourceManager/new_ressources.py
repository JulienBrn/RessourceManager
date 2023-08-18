from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools
from RessourceManager.lifting import Lifting
from RessourceManager.id_makers import unique_id, make_result_id
from RessourceManager.storage import Storage
import inspect

logger = logging.getLogger(__name__)


class InputOptions:
    dependency: Literal("RessourceId") | Literal("Value") | Literal("Ignore")
    make_id: Callable[[Any], str]
    pass_as: Literal("Value") | Literal("Ressource") | Storage 
    action: Literal("Used") | Literal("Passed") | Literal("Other")
    exception: Literal("Propagate") | Literal("PassAsValue")
    lifting: Lifting

class ResultOptions:
    result_on: Literal("Return") | (Storage, str) #str is the parameter that should be used to indicate where the result should be stored
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
    # depends_on: Set[RessourceData]
    # is_used_by: Set[RessourceData]

    # Possibly shared

        ## Storage options
    readers: List[Storage]
    writers: List[Storage]

        ## Function Definition
    group_name: str
    f: Callable[..., Any]
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

    def _load(self):
        excpts = {}
        for storage in self.readers:
            if storage.has(self):
                try:
                    res = storage.load(self)
                    self.log.append(dict(action="loading_ressource", storage=storage, time=None, computation_time=None, n_errors=0, n_warnings=0))
                    return res
                except Exception as e:
                    try:
                        raise LoadingRessourceError(f"Impossible to read ressource from storage {storage} where it is stored") from e
                    except LoadingRessourceError as exc:
                        excpts[storage.name] = exc
        if excpts == []:
            raise LoadingRessourceError(f"Impossible to read ressource {self.identifier}: ressource is not stored on any read storages")
        else:
            # try:
            raise ExceptionGroup(f"Impossible to read ressource {self.identifier}. Ressource was stored on {excpts.keys()}, but all storages had loading errors", excpts.values())
            # except ExceptionGroup as e:
            #     raise LoadingRessourceError(f"Impossible to read ressource {self.identifier}: Ressource was stored on {excpts.keys()}, but all storages had loading errors") from e
    
    
    def _get_params(self):
        param_values = {k:self.compute_param_value(k) for k in self.param_dict.keys()}
        propagated_exceptions =[]
        for k in param_values.keys():
            if self.param_dict[k][1].exception=="Propagate" and isinstance(param_values[k], BaseException):
                try:
                    raise param_values[k]
                except Exception  as e:
                    try:
                        raise InputRessourceError(f"Error in input {k} while computing ressource {self.identifier}") from e
                    except InputRessourceError as exc:
                        propagated_exceptions.append(exc)
        if len(propagated_exceptions) == 1:
            raise propagated_exceptions[0]
        elif len(propagated_exceptions) > 1:
            raise ExceptionGroup("Errors during computation of parameters for ressource {self.identifier}", propagated_exceptions)
        else:
            if self.result_options.result_on != "Return":
                param_values[self.result_options.result_on[1]] = self.get_location(self.result_options.result_on[0])
            return param_values
    
    def _compute(self, param_values):
        self.log.append(dict(action="computation_start"))
        try:
            res = self.f(**param_values)
        except Exception  as e:
            try:
                raise ComputationRessourceError(f"Error in while computing ressource {self.identifier}") from e
            except ComputationRessourceError as excpt:
                res = excpt
        self.log.append(dict(action="computation_end"))

        if self.result_options.result_on != "Return":
            if not self.result_options.result_on[0].has(self):
                res = MissingRessourceError(f"Computation of ressource {self.identifier} should have stored result on storage {self.result_options.result_on}, but no ressource found")
        return res
    
    def _store(self, res):
        for storage in self.writers:
            if not storage.has(self):
                try:
                    storage.dump(self, res)
                    self.log.append(dict(action="writing ressource", storage=storage, result=res))
                    return res
                except Exception as e:
                    logger.exception("Impossible to write ressource to storage. Skipping storage {}", e)
                
            
    def _get(self):
        try:
            res = self._load()
        except * LoadingRessourceError:
            try:
                params = self._get_params()
            except * InputRessourceError as e:
                res = e
            else:
                try:
                    res = self._compute(params)
                except * ComputationRessourceError as e:
                    res = e
                else:
                    if self.result_options.result_on != "Return":
                        try:
                            res = self._load()
                        except * LoadingRessourceError as e:
                            res = e
            try:
                self._store(res)
            except Exception as e:
                logger.exception(f"Problem while attempting to store ressource {self.identifier} to storage", e)
        return res

    def write_on_storage(self, s: Storage):
        if not s.has(self):
            try:
                res = self._load()
            except * LoadingRessourceError:
                try:
                    params = self._get_params()
                except * InputRessourceError as e:
                    res = e
                else:
                    try:
                        res = self._compute(params)
                    except * ComputationRessourceError as e:
                        res = e
                    else:
                        if self.result_options.result_on != "Return":
                            if s.has(self):
                                return
                            else:
                                try:
                                    res = self._load()
                                except * LoadingRessourceError as e:
                                    res = e
                try:
                    self._store(res)
                except Exception as e:
                    logger.exception(f"Problem while attempting to store ressource {self.identifier} to storage", e)
            if not s.has(self):
                s.dump(res)

    def result(self, raise_on_exception=True):
        res = self._get()
        if isinstance(res, Exception) and raise_on_exception:
             raise res
        else:
             return res
        
    # def invalidate(self):
    #     pass

    # def remove_from_storage(self, s: Storage):
    #     pass

    


             























        # def get_id_from_ressource(r: RessourceData):
        #     if r.result_options.is_value_dependency:
        #         try:
        #             id = r.result()
        #         except:
        #             raise #TODO
        #     else:
        #         return r.id
        
        

        # self.id = str(self.group_name) + mhash({k:v for k,v in arg_dict.items() if not params_df.loc[k, "ignore"]})[len("dict"):]

        


class RessourceDeclarator:
    def __init__(self, name, result_options, compute_options, readers, writers, params, f):
        self.name = name
        self.result_options=result_options
        self.compute_options = compute_options
        self.readers = readers
        self.writers = writers
        self.params = params
        self.f = f

    def declare(self, *args, **kwargs):
        s = inspect.signature(self.f).bind(*args, **kwargs)
        s.apply_defaults()
        arg_dict = s.arguments

        r = RessourceData()
        r.group_name = self.name
        r.compute_options = self.compute_options
        r.result_options = self.result_options
        r.param_dict = {k:(arg_dict[k], self.params[k]) for k in self.params}
        r.f = self.f
        r.readers = self.readers
        r.writers = self.writers
        r.log = []
        return r


class RessourceDecorator:
    def __init__(self, name = None, *, result_on = "Return", make_result_id = make_result_id, compute_options = ComputeOptions(), readers=[MemoryStorage(), PickledDiskStorage(".cache")], writers = [MemoryStorage(), PickledDiskStorage(".cache")]):
        self.name = name
        self.result_options= ResultOptions(result_on = result_on, make_result_id = make_result_id)
        self.compute_options = compute_options
        self.inputopt={".all": InputOptions()}
        self.readers=readers
        self.writers=writers

    def __call__(self, f): 
        arg_names = set(inspect.signature(f).parameters.keys())

        if not (set(self.inputopt.keys()) - set(arg_names.keys()) - set([".all"])).empty():
            raise ValueError(f"Invalid parameters named for decorator: {(set(self.inputopt.keys()) - set(arg_names.keys()) - set(['.all']))}")
        params = {}

        for k in arg_names:
            if k not in self.inputopt:
                params[k] = self.inputopt[".all"]
            else:
                params[k] = self.inputopt[k]
        

        return RessourceDeclarator(self.name, self.result_options, self.compute_options, self.readers, self.writers, params, f)



    def params(self, params = ".all", *, dependency = None,  make_id = None, pass_as=None, action= None, exception = None, lifting = None, vectorized=None):
        def update_inputopt(o):
            if not dependency is None:
                o.dependency = dependency
            if not make_id is None:
                o.make_id = make_id
            if not pass_as is None:
                o.pass_as = pass_as
            if not action is None:
                o.action = action
            if not exception is None:
                o.exception = exception
            if not lifting is None:
                o.lifting = lifting
            if not vectorized is None:
                o.vectorized = vectorized

        if params ==".all":
            params = list(self.inputopt.keys())
        if isinstance(params, str):
            params = [params]

        other = self.copy()
        for param in params:
            if not param in other.inputopt:
                other.inputopt[param] = other.inputopt[".all"].copy()
            update_inputopt(other.inputopt[param])

        return other
    
    def name(self, name: str):
        other = self.copy()
        other.name = name
        return other
    
    def result_options(self, result_on = None, make_id = None):
        other = self.copy()
        if not result_on is None:
            other.result_options.result_on = result_on
        if not make_id is None:
            other.result_options.make_result_id = make_id
        return other
    
    def readers(self, *args):
        other = self.copy()
        other.readers = args
        return other

    def add_readers(self, *args, with_highest_priority = True):
        other = self.copy()
        if with_highest_priority:
            other.readers = args + other.readers
        else:
            other.readers = other.readers + args
        return other
    
    def writers(self, *args):
        other = self.copy()
        other.writers = args
        return other

    def add_writers(self, *args, with_highest_priority = True):
        other = self.copy()
        if with_highest_priority:
            other.writers = args + other.writers
        else:
            other.readers = other.writers + args
        return other


