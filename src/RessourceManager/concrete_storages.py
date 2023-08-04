from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn, Protocol, Optional
import io as iom, pathlib, multiprocessing, inspect, functools
from RessourceManager.abstract_storages import *
import dill

class FileStorage(AbstractStorage):
    def __init__(self, dir: pathlib.Path):
        super().__init__(f"FileStorage at {dir}")
        self.dir = pathlib.Path(dir)

    def get_ressource_location(self, uniqueid: AlphaNumString, name_info: Dict[str, str]) -> str: 
        extension = name_info["extension"] if "extension" in name_info else ".unknown"
        group = name_info["group"] if "group" in name_info else ""
        return str(pathlib.Path(group)/ uniqueid + extension if group else uniqueid + extension)

    def has_ressource(self, location: str) -> bool:
        return (self.dir / location).exists()
    
    def mk_write_io(self, io_method: str, location: str) -> IO:
        if io_method == "pathlib_path":
            return (self.dir / location)
        elif io_method == "text_io":
            return (self.dir / location).open("w")
        elif io_method == "bytes_io":
            return (self.dir / location).open("wb")
        else:
            raise NotImplementedError(f"IO Method {io_method} is not implemented in FileStorage")
    
    def mk_read_io(self, io_method: str, location: str) -> IO:
        if io_method == "pathlib_path":
                return (self.dir / location)
        elif io_method == "text_io":
            return (self.dir / location).open("r")
        elif io_method == "bytes_io":
            return (self.dir / location).open("rb")
        else:
            raise NotImplementedError(f"IO Method {io_method} is not implemented in FileStorage")
    
    def supports_iomethod(method: str) -> bool:
       return method in ["pathlib_path", "text_io", "bytes_io"]
    
    def remove(self, locations: Set[str], missing_ok=False) -> NoReturn:
        for loc in locations:
            (self.dir / loc).unlink(missing_ok=missing_ok)
    
    @property
    def space(self) -> SpaceInformation:
        raise NotImplementedError()
    
    @property
    def lifetime(self) -> LifetimeInformation:
        class MLifeTime(LifetimeInformation):
            def persists_after_execution(self) -> bool:
                return True
        return MLifeTime()
    


class DictMemoryStorage(AbstractStorage):
    def __init__(self, d: Optional[Dict[str, Any]]):
        
        self.d = d if not self.d is None else {}
        super().__init__(f"DictMemoryStorage at {hex(id(self.d))}")

    def _update_dict(self, location, obj):
        self.d[location] = obj

    def get_ressource_location(self, uniqueid: AlphaNumString, name_info: Dict[str, str]) -> str: 
        extension = name_info["extension"] if "extension" in name_info else ".unknown"
        group = name_info["group"] if "group" in name_info else ""
        return str(pathlib.Path(group)/ uniqueid + extension if group else uniqueid + extension)

    def has_ressource(self, location: str) -> bool:
        return location in self.d
    
    def mk_write_io(self, io_method: str, location: str) -> IO:
        if io_method == "function":
            return lambda obj: self.update(location, obj)
        else:
            raise NotImplementedError(f"IO Method {io_method} is not implemented in {self.name}")
    
    def mk_read_io(self, io_method: str, location: str) -> IO:
        if io_method == "function":
                return lambda: self.d[location]
        else:
            raise NotImplementedError(f"IO Method {io_method} is not implemented in {self.name}")
    
    def supports_iomethod(method: str) -> bool:
       return method in ["function"]
    
    def remove(self, locations: Set[str], missing_ok=False) -> NoReturn:
        if missing_ok:
            for loc in locations:
                self.d.pop(loc) 
        else:
            for loc in locations:
                self.d.pop(loc, 0) 
    
    @property
    def space(self) -> SpaceInformation:
        raise NotImplementedError()
    
    @property
    def lifetime(self) -> LifetimeInformation:
        class MLifeTime(LifetimeInformation):
            def persists_after_execution(self) -> bool:
                return False
        return MLifeTime()

import pickle   
class PickleProtocol(Protocol):

    def __init__(self):
        super().__init__("pickle")

    def supports_dtype_method(t : type, io_method: str) -> bool:
        return io_method in ["pathlib_path", "bytes_io"]

    def dump(self, io: IO, obj: Any) -> NoReturn:
        if isinstance(io, pathlib.Path):
            with io.open("wb") as f:
                pickle.dump(obj, f)
        elif isinstance(io, iom.BytesIO):
            pickle.dump(obj, io)
        else:
            raise NotImplementedError(f"Dump method is not implemented in PickleProtocol for io of type {type(io)}")

    def load(self, io: IO) -> Any:
        if isinstance(io, pathlib.Path):
            with io.open("rb") as f:
                return pickle.load(f)
        elif isinstance(io, iom.BytesIO):
            return pickle.load(io)
        else:
            raise NotImplementedError(f"Load method is not implemented in PickleProtocol for io of type {type(io)}")


class FunctionProtocol(Protocol):
    def __init__(self):
        super().__init__("functionIO")

    def supports_dtype_method(t : type, io_method: str) -> bool:
        return io_method in ["function"]

    def dump(self, io: Callable[[Any], ], obj: Any) -> NoReturn:
        io(obj)

    def load(self, io: Callable[[], Any]) -> Any:
        return io()