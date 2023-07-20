from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools, hashlib


from RessourceManager.storage_solution import Storage, Loader, ChannelTypeParam, RessourceIDType, Channel, StorageLocation, Predicate

class DictStorage(Storage[RessourceIDType]):
    name: str
    space_limitations: float
    storage_lifetime: float
    is_human_readable: bool

    supported_channels: List[ChannelTypeParam] = lambda self, x: x=="Dict"

    def __init__(self):
        self.d = {}
        self.name = "dict_storage"

    def mk_write_channel(self, channel: ChannelTypeParam, loader: Loader, uniqueid: RessourceIDType, name_info: Any = None) -> Union[Channel, StorageLocation]:
        from RessourceManager.ressource_manager import Hasher
        loc = str(name_info) + Hasher().mk_string_hash([channel, loader, uniqueid])
        return ((self.d, loc), loc)
    
    def mk_read_channel(self, channel: ChannelTypeParam, loader: Loader, location: StorageLocation) -> Channel:
        return (self.d, location)
    
    def remove(self, location: List[StorageLocation]) -> NoReturn:
        raise NotImplementedError()
    
    def available_space(self, location: List[StorageLocation]) -> float:
        raise NotImplementedError()
    


class DictLoader(Loader):
    name: str
    supported_type_channels: Predicate[Tuple[type, ChannelTypeParam]] = lambda self, t, c: c == "Dict"
    def __init__(self):
        self.name = "dict_loader"
    def dump(self,  channel: Channel, obj: Any) -> None:
        (d, k) = channel
        d[k] = obj
    def load(self, channel: Channel) -> Any:
        (d, k) = channel
        return d[k]