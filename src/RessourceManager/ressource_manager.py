from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools, hashlib
from RessourceManager.storage_solution import StorageSolution, StorageLocation, Storage, Loader, ChannelTypeParam, Predicate, RessourceIDType
import collections.abc, logging

logger = logging.getLogger(__name__)

GroupType = NewType("GroupType", object)

def log_decorator(func):
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__} with args={args} kwargs={kwargs}")
        result = func(*args, **kwargs)
        print(f"{func.__name__} returned {result}")
        return result
    return wrapper
    
class Hasher:
    name: str
    supported_types: Predicate[Tuple[type]]
    # @log_decorator
    def mk_string_hash(self, obj: Any) -> RessourceIDType:
        if isinstance(obj, str):
            return obj
        if isinstance(obj, int) or isinstance(obj, float):
            return str(obj)
        if isinstance(obj, Storage):
            return obj.name
        if isinstance(obj, Loader):
            return obj.name
        if isinstance(obj, Ressource):
            return obj.id
        if isinstance(obj, collections.abc.Sequence):
           return hashlib.sha256(".".join([self.mk_string_hash(e) for e in obj]).encode()).hexdigest()
        if isinstance(obj, dict):
           return hashlib.sha256(".".join(sorted([self.mk_string_hash(e) for e in obj.items()])).encode()).hexdigest()
        raise NotImplementedError(f"Type {type(obj)} not yet supported in hash")

class ComputingRestriction: pass


class Ressource:
    group: GroupType
    storage_locations: Dict[StorageSolution, StorageLocation]
    run_output: StorageSolution
    run_restrictions: List[ComputingRestriction]
    upstream_dependencies: Dict[str, Ressource]
    downstream_dependencies: Set[Ressource]

    id: RessourceIDType

    def store(self, s: StorageSolution):
        raise NotImplementedError()
    
    def mk_id(self) -> NoReturn:
        raise NotImplementedError()


from RessourceManager.ressources import RessourceDecorator
class RessourceManager:
    storages: List[Storage]
    loaders: List[Loader]

    ressources: Set[Ressource]

    @functools.wraps(RessourceDecorator.__init__)
    def ressource(self,**kwargs):
        return RessourceDecorator(**kwargs)
    
    def compute(self, l: List[Ressource]) -> NoReturn:
        pass