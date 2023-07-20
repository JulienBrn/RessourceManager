from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools

PersistenceRestriction = Union[Literal["limitedspace"], Literal["execution"]]
StorageLocation = TypeVar("StorageLocation")

ChannelTypeParam = Union[Literal["binarystream"], Literal["characterstream"], Literal["path"], Literal["Dict"], Literal["Pipe"],]
ChannelType = Union[io.BytesIO, io.StringIO, pathlib.Path, Tuple[Dict[StorageLocation, Any], StorageLocation], multiprocessing.Pipe]
T = TypeVar("T")
Predicate = Union[Set[T], Callable[[T], bool]]

RessourceIDType = NewType("RessourceId", str)
GroupType = NewType("GroupType", object)

class Channel:
    channel: ChannelType
    channel_type: ChannelTypeParam


class Storage(Generic[StorageLocation]):
    name: str
    persistence: List[PersistenceRestriction]
    is_human_readable: bool
    supported_channels: List[ChannelType]

    def mk_write_channel(self, channel: ChannelTypeParam, uniqueid: RessourceIDType, name_info: Any = None) -> Union[Channel, StorageLocation]:
        raise NotImplementedError()
    def mk_read_channel(self, channel: ChannelTypeParam, location: StorageLocation) -> Channel:
        raise NotImplementedError()
    
    def remove(self, location: List[StorageLocation]) -> NoReturn:
        raise NotImplementedError()
    
    def available_space(self, location: List[StorageLocation]) -> float:
        raise NotImplementedError()
    


class Loader:
    name: str
    supported_type_channels: Predicate[Tuple[type, ChannelType]]

    def dump(self, obj: Any, channel: Channel) -> None:
        raise NotImplementedError()
    def load(self, channel: Channel) -> Any:
        raise NotImplementedError()
    
class Hasher:
    name: str
    supported_types: Predicate[Tuple[type]]
    def mk_string_hash(self, obj: Any) -> RessourceIDType:
        raise NotImplementedError()

class ComputingRestriction: pass


class Ressource:
    group: GroupType
    storage_locations: Dict[Storage, StorageLocation]
    run_output: Storage
    run_restrictions: List[ComputingRestriction]
    upstream_dependencies: Dict[str, Ressource]
    downstream_dependencies: Set[Ressource]

    id: RessourceIDType

    def compute(self, s: Storage = None):
        raise NotImplementedError()
    def mk_id(self) -> NoReturn:
        raise NotImplementedError()

class BasicRessource(Ressource):
    def __init__(self, f, arg_dict):
        self.f = f
        self.arg_dict = arg_dict

    def compute(self, s: Storage = None):
        return self.f(**self.arg_dict)

class Proxy:
    def __init__(self, f):
        self.f = f
    def rec(self, *args, **kwargs):
        """Proxy object
        """
        return self.f(*args, **kwargs)


class RessourceDecorator:
    def __init__(auto_save_on = None, loaders = None, run_restrictions=None, group=None, run_output = "return", vectorize = None, unload = None):
        pass

    def param(self, str, ignore=False, dtype=None, vectorize = None):
        pass
    
    def return_value(self, run_output = "return", auto_save_on = None, loaders = None, group=None, unload = None):
        pass
    
    def run(self, run_restrictions=None):
        pass

    def __call__(self, f):
        @functools.wraps(f)
        def new_f(*args, **kwargs):
            s = inspect.signature(f).bind(*args, **kwargs)
            s.apply_defaults()
            return BasicRessource(f, s.arguments)
        return Proxy(new_f)
    

class RessourceManager:
    storages: List[Storage]
    loaders: List[Loader]

    ressources: Set[Ressource]

    @functools.wraps(RessourceDecorator.__init__)
    def ressource(self,**kwargs):
        return RessourceDecorator(**kwargs)
    
    def compute(self, l: List[Ressource]) -> NoReturn:
        pass