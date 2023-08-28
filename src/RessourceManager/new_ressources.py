from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools
from RessourceManager.lifting import Lifting, NoLifting
import RessourceManager.lifting
from RessourceManager.id_makers import unique_id, make_result_id
from RessourceManager.storage import Storage, memory_storage, pickled_disk_storage
import inspect, pathlib, traceback, datetime
from dataclasses import dataclass

logger = logging.getLogger(__name__)
exlog = pathlib.Path('./exception_log.txt')
if not exlog.exists():
    exlog.parent.mkdir(parents=True, exist_ok=True)
exlog.open("a").write(f"\nStarting log for run launched at date {datetime.datetime.now()}\n\n")

@dataclass
class InputOptions:
    dependency: Literal("RessourceId") | Literal("Value") | Literal("Ignore")
    make_id: Callable[[Any], str]
    pass_as: Literal("Value") | Literal("Ressource") | Storage 
    action: Literal("Used") | Literal("Passed") | Literal("Other")
    exception: Literal("Propagate") | Literal("PassAsValue")
    lifting: Lifting
    _vectorized: bool

@dataclass
class ResultOptions:
    result_on: Literal("Return") | (Storage, str) #str is the parameter that should be used to indicate where the result should be stored
    make_id: Callable[[str, Dict[str, str], bool], str]

@dataclass
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
class VectorizationError(Exception):pass

@dataclass
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
    
    def __repr__(self):
        return self.identifier
    
    def __str__(self):
        return self.identifier[0:50] + ('...)' if len(self.identifier) > 49 else '')
    
    def compute_param_value(self, param: str, progress):
        option = self.param_dict[param][1]
        param_value = self.param_dict[param][0]
        ressources, reconstruct = option.lifting.deconstruct(param_value)

        match (option.action, option.pass_as):
            case "Used", "Value":
                values = []
                for r in ressources:
                    values.append(r._get(progress))
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
        
        res = reconstruct(values)
        self.log.append(dict(action="Computed param value", param=param, val=res, values=values))
        return res
    
    def compute_param_id(self, param: str, for_storage: bool) -> str:
        option = self.param_dict[param][1]
        param_value = self.param_dict[param][0]
        ressources, reconstruct = option.lifting.deconstruct(param_value)

        match option.dependency, for_storage:
            case "Ignore", _:
                raise Exception("Compute_param_id called on ignored parameter")
            case ("RessourceId", _) | ("Value", False):
                ids = []
                for r in ressources:
                    ids.append(r.get_id(for_storage))
            case ("Value", True):
                ids = []
                for r in ressources:
                    ids.append(r._get(progress=None))
            case _:
                raise NotImplementedError(f"Unknown input option: dependency={option.dependency}")
            
        obj = reconstruct(ids)
        return option.make_id(obj)
    
    def compute_id(self, for_storage: bool):
        try:
            param_id_dict={k: self.compute_param_id(k, for_storage) for k,(v, opt) in self.param_dict.items() if not opt.dependency=="Ignore"}
            id = self.result_options.make_id(self.group_name, param_id_dict, for_storage)
        except Exception as e:
                self.log.append(dict(action="computing_id", result=e, time=None, computation_time=None, n_errors=1, n_warnings=0))
                raise RessourceIDError(f"Error while computing id for ressource of group {self.group_name}") from e
        self.log.append(dict(action="computing_id", for_storage=for_storage, result=id, time=None, computation_time=None, n_errors=0, n_warnings=0))
        return id

    @functools.cached_property
    def identifier(self):
        return self.compute_id(for_storage=False)
    
    @functools.cached_property
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
        if excpts == {}:
            raise LoadingRessourceError(f"Impossible to read ressource {self.identifier}: ressource is not stored on any read storages")
        else:
            # try:
            raise ExceptionGroup(f"Impossible to read ressource {self.identifier}. Ressource was stored on {excpts.keys()}, but all storages had loading errors", list(excpts.values()))
            # except ExceptionGroup as e:
            #     raise LoadingRessourceError(f"Impossible to read ressource {self.identifier}: Ressource was stored on {excpts.keys()}, but all storages had loading errors") from e
    
    
    def _get_params(self, progress):
        self.log.append(dict(action="computating_params_start"))
        param_values = {k:self.compute_param_value(k, progress) for k in self.param_dict.keys()}
        propagated_exceptions =[]
        for k in param_values.keys():
            if self.param_dict[k][1].exception=="Propagate" and isinstance(param_values[k], BaseException):
                try:
                    raise param_values[k]
                except Exception  as e:
                    try:
                        raise InputRessourceError(f"Error in input '{k}' while computing ressource {self.identifier}") from e
                    except InputRessourceError as exc:
                        propagated_exceptions.append(exc)
        if len(propagated_exceptions) == 1:
            raise propagated_exceptions[0]
        elif len(propagated_exceptions) > 1:
            raise ExceptionGroup("Errors during computation of parameters for ressource {self.identifier}", propagated_exceptions)
        else:
            if self.result_options.result_on != "Return":
                param_values[self.result_options.result_on[1]] = self.get_location(self.result_options.result_on[0])
            self.log.append(dict(action="computating_params_end", value = param_values))
            return param_values
    
    def _compute(self, param_values, progress):
        self.log.append(dict(action="computation_start"))
        try:
            if self.compute_options.progress is None:
                res = self.f(**param_values)
            else:
                if inspect.isclass(progress):
                    progress = progress()
                progress.set_description(f"Computing {self}")
                res = self.f(**param_values, **{self.compute_options.progress: progress})
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
                    if not storage.has(self):
                        raise MissingRessourceError(f"Store failed for storage {storage}")
                    self.log.append(dict(action="writing ressource", storage=storage, result=res))
                except Exception as e:
                    logger.warning(f"Impossible to write ressource to storage. Skipping storage {storage}. Exception traceback saved in file 'exception_log.txt'")
                    exlog = pathlib.Path('exception_log.txt')
                    exlog.open("a").write(f"\nError storing ressource {self.identifier}\n{traceback.format_exc()}\n\n")
                
            
    def _get(self, progress):
        try:
            res = self._load()
            return res
        except * LoadingRessourceError:
            pass
        try:
            params = self._get_params(progress=progress)
        except * InputRessourceError as e:
            res = e
        else:
            try:
                res = self._compute(params, progress=progress)
            except * ComputationRessourceError as e:
                try:
                    raise e 
                except Exception as e:
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
        is_loaded = True
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
                            try:
                                res = self._load()
                            except * LoadingRessourceError as e:
                                if not s.has(self):
                                    res = e
                                else:
                                    is_loaded = False
                            
                try:
                    if is_loaded:
                        self._store(res)
                except Exception as e:
                    logger.exception(f"Problem while attempting to store ressource {self.identifier} to storage", e)
            if not s.has(self):
                s.dump(res)

    def result(self, exception: (Literal("raise") | Literal("return") | List[Exception]) ="raise", progress=tqdm.tqdm ):
        res = self._get(progress=progress)
        if isinstance(res, Exception):
            if exception=="raise":
                raise res
            elif exception=="return":
                return res
            elif isinstance(exception, list):
                exception.append(res)
                return "Error"
            else:
                raise ValueError(f"Unknown option {exception}")
        else:
             return res
        
    # def invalidate(self):
    #     pass

    # def remove_from_storage(self, s: Storage):
    #     pass

RessourceManager.lifting.RessourceData = RessourceData
import copy
class RessourceManager:
    def __init__(self):
        self.d={}
    def declare(self, r):
        if not r.identifier in self.d:
            self.d[r.identifier] = r
        return self.d[r.identifier]

default_manager = RessourceManager()

class RessourceDeclarator:
    def __init__(self, name, result_options, compute_options, readers, writers, params, f, manager):
        self.name = name
        self.result_options=result_options
        self.compute_options = compute_options
        self.readers = readers
        self.writers = writers
        self.param_dict = params
        self.f = f
        self.manager = manager

    def copy(self):
        return copy.deepcopy(self)
    
    def declare(self, *args, **kwargs):
        if self.compute_options.progress is None:
            s = inspect.signature(self.f).bind(*args, **kwargs)
        else:
            s = inspect.signature(self.f).bind(*args, **kwargs, **{self.compute_options.progress:None})
        s.apply_defaults()
        arg_dict = s.arguments
        vectorized_args = [k for k,v in self.param_dict.items() if v._vectorized]
        # print(f"vectorized args = {vectorized_args}", self.param_dict)
        lens = {k:len(arg_dict[k]) for k in vectorized_args}
        if len(set(lens.values())) > 1:
            raise VectorizationError(f"All vectorized arguments should have same length. Got {lens}")
        if vectorized_args != []:
            l=[]
            for i in range(list(lens.values())[0]):
                r = RessourceData(
                    group_name = self.name, compute_options=self.compute_options, result_options=self.result_options, 
                    param_dict={k:(arg_dict[k] if not k in vectorized_args else arg_dict[k][i], self.param_dict[k]) for k in self.param_dict}, f = self.f, readers=self.readers, writers=self.writers, log=[])
                l.append(self.manager.declare(r))
            return l
        else:
            r = RessourceData(
                group_name = self.name, compute_options=self.compute_options, result_options=self.result_options, 
                param_dict={k:(arg_dict[k], self.param_dict[k]) for k in self.param_dict}, f = self.f, readers=self.readers, writers=self.writers, log=[])
        return self.manager.declare(r)
    
    def __call__(self, *args, **kwargs):
        return self.declare(*args, **kwargs)
    
    def params(self, params = ".all", *, dependency = None,  make_id = None, pass_as=None, action= None, exception = None, lifting = None, vectorized=None):
        # print(self.param_dict, params)
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
                o._vectorized = vectorized

        if params ==".all":
            params = list(self.param_dict.keys())
        if isinstance(params, str):
            params = [params]

        if (set(params) - set(self.param_dict.keys())) == {}:
            raise ValueError(f"Invalid parameters named for decorator: {(set(params) - set(self.param_dict.keys()))}")
        other = self.copy()
        for param in params:
            update_inputopt(other.param_dict[param])
        # print(other.param_dict, params)
        return other

class RessourceDecorator:
    def __init__(self, name = None, *, manager = default_manager, result_on = "Return", make_result_id = make_result_id, compute_options = None, readers=[memory_storage, pickled_disk_storage], writers = [memory_storage, pickled_disk_storage]):
        self.name = name
        self.result_options= ResultOptions(result_on = result_on, make_id = make_result_id)
        self.compute_options = ComputeOptions(n_retries=1, progress = None, alternative_paths=[])
        self.inputopt={".all": InputOptions(dependency="RessourceId", make_id = unique_id, pass_as = "Value", action="Used", lifting=NoLifting(), exception="Propagate", _vectorized=False)}
        self.readers=readers
        self.writers=writers
        self.manager = manager

    def copy(self):
        return copy.deepcopy(self)
    
    def __call__(self, f): 
        arg_names = set(inspect.signature(f).parameters.keys())
        ignored_params = []
        if self.compute_options.progress is None:
            ftqdm = {a for a in arg_names if "tqdm" in a}
            if len(set(ftqdm) - set(self.inputopt.keys())) == 1:
                self.compute_options.progress = (set(ftqdm) - set(self.inputopt.keys())).pop()
            else:
                self.compute_options.progress = None
        if not self.compute_options is None:
            ignored_params.append(self.compute_options.progress)


        if self.name is None:
            self.name = f.__name__
        if (set(self.inputopt.keys()) - set(arg_names) - set([".all"])) == {}:
            raise ValueError(f"Invalid parameters named for decorator: {(set(self.inputopt.keys()) - set(arg_names.keys()) - set(['.all']))}")
        params = {}

        for k in arg_names:
            if k in ignored_params:
                continue
            if k not in self.inputopt:
                params[k] = copy.copy(self.inputopt[".all"])
            else:
                params[k] = self.inputopt[k]

        
        

        return RessourceDeclarator(self.name, self.result_options, self.compute_options, self.readers, self.writers, params, f, self.manager)



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
                other.inputopt[param] = other.inputopt[".all"].deepcopy()
            update_inputopt(other.inputopt[param])

        return other
    
    def name(self, name: str):
        other = self.copy()
        other.name = name
        return other
    
    def manager(self, manager: RessourceManager):
        other = self.copy()
        other.manager = manager
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
        other.readers = list(args)
        return other

    def add_readers(self, *args, with_highest_priority = True):
        other = self.copy()
        if with_highest_priority:
            other.readers = list(args) + other.readers
        else:
            other.readers = other.readers + list(args)
        return other
    
    def writers(self, *args):
        other = self.copy()
        other.writers = list(args)
        return other

    def add_writers(self, *args, with_highest_priority = True):
        other = self.copy()
        if with_highest_priority:
            other.writers = list(args) + other.writers
        else:
            other.readers = other.writers + list(args)
        return other


