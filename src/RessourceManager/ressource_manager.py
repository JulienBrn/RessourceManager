from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing

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

    def run() -> NoReturn:
        raise NotImplementedError()
    def mk_id(self) -> NoReturn:
        raise NotImplementedError()

class RessourceDecorator:
    def __init__(storages = None, loaders = None, run_restrictions=None, group=None, run_output = "return", vectorize = None):
        pass

    def param(self, str, ignore=False, dtype=None, vectorize = None):
        pass
    
    def return_value(self, run_output = "return", storages = None, loaders = None, group=None):
        pass
    
    def run(self, run_restrictions=None):
        pass

    def __call__(self, f):
        def new_f(*args, **kwargs):
            return Ressource(f)
        return new_f
    

class RessourceManager:
    storages: List[Storage]
    loaders: List[Loader]

    ressources: Set[Ressource]

    def ressource(self,**kwargs):
        return RessourceDecorator(**kwargs)