from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools

StorageLocation = TypeVar("StorageLocation")

ChannelTypeParam = Union[Literal["binarystream"], Literal["characterstream"], Literal["path"], Literal["Dict"], Literal["Pipe"],]
Channel = Union[io.BytesIO, io.StringIO, pathlib.Path, Tuple[Dict[StorageLocation, Any], StorageLocation], multiprocessing.Pipe]
T = TypeVar("T")
Predicate = Union[Set[T], Callable[[T], bool]]

RessourceIDType = NewType("RessourceId", str)
AlphaNumString = NewType("AlphaNumString", str)

class Storage(Generic[StorageLocation]):
    name: str
    space_limitations: float
    storage_lifetime: float
    is_human_readable: bool

    supported_stream_types: List[ChannelTypeParam]

    def mk_ressource_location(self, uniqueid: RessourceIDRepresentationType, name_info: Any = None) -> StorageLocation:
        raise NotImplementedError()

    def has_ressource(loc: StorageLocation):
        raise NotImplementedError()
    
    def mk_write_channel(self, channel: ChannelTypeParam, location: StorageLocation) -> Channel:
        raise NotImplementedError()
    
    def mk_read_channel(self, channel: ChannelTypeParam, location: StorageLocation) -> Channel:
        raise NotImplementedError()
    
    def remove(self, location: List[StorageLocation]) -> NoReturn:
        raise NotImplementedError()
    
    def available_space(self, location: List[StorageLocation]) -> float:
        raise NotImplementedError()
    


class Loader:
    name: str
    supported_type_channels: Predicate[Tuple[type, ChannelTypeParam]]

    def dump(self,  channel: Channel, obj: Any) -> None:
        raise NotImplementedError()
    def load(self, channel: Channel) -> Any:
        raise NotImplementedError()
    

class StorageSolution:
    loader: Loader
    storage: Storage
    channel: ChannelTypeParam

    def __init__(self, loader: Loader, storage: Storage, channel: ChannelTypeParam):
        self.loader, self.storage, self.channel = loader, storage, channel
        if not self.storage.supported_channels(self.channel):
            raise ValueError(f"Unsupported channel type {channel} for storage f{storage}")

    def store(self, obj: Any, id: RessourceIDType, info: Any=None) -> StorageLocation:
        if not self.loader.supported_type_channels(type(obj), self.channel):
            raise ValueError(f"Loader {self.loader} does not support datatype {type(obj)} on channel {self.channel}")
        chan, loc = self.storage.mk_write_channel(self.channel, self.loader, id, info)
        self.loader.dump(chan, obj)
        return loc
    
    def load(self, loc: StorageLocation) -> Any:
        chan = self.storage.mk_read_channel(self.channel, self.loader, loc)
        obj = self.loader.load(chan)
        return  obj
    
    def transfert(self, dest, loc: StorageLocation, id: RessourceIDType, info: Any = None) -> Any:
        obj = self.load(loc)
        dest_loc = self.store(obj, id, info)
        return  dest_loc
    
    def has_ressource(self, loc: StorageLocation):
        raise NotImplementedError()