from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn
import io, pathlib, multiprocessing, inspect, functools

from RessourceManager.ressource_manager import Ressource, Storage, Loader, ChannelTypeParam, StorageSolution, Hasher
from RessourceManager.dict_storage_solution import DictStorage, DictLoader

class BasicRessource(Ressource):
    def __init__(self, f, arg_dict, storage_solutions):
        self.f = f
        print(arg_dict)
        self.arg_dict = arg_dict
        self.storage_locations = {k:None for k in storage_solutions}
        self.run_output = "return_value"
        self.group = "toto"

    def store(self, dest: StorageSolution):
        if self.storage_locations[dest] != None:
            return
        computed = {k:v for k,v in self.storage_locations.items() if not v is None}
        if len(computed) > 0:
            storage, loc = list(computed.items())[0]
            new_loc = storage.transfert(dest, loc, self.id, self.group)
            self.storage_locations[dest] = new_loc
        elif self.run_output == "return_value":
            val = self.f(**self.arg_dict)
            loc = dest.store(val, self.id, self.group)
            self.storage_locations[dest] = loc
        else:
            raise NotImplementedError()
        
    def get(self):
        computed = {k:v for k,v in self.storage_locations.items() if not v is None}
        if len(computed) > 0:
            storage, loc = list(computed.items())[0]
        else:
            self.store(list(self.storage_locations.keys())[0])
            storage, loc = list(self.storage_locations.items())[0]
        return storage.load(loc)
    
    @property
    def id(self):
        res = Hasher().mk_string_hash([self.group, self.arg_dict])
        print(res)
        return res


        


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

    def param(self, str, ignore=False, dtype=None, vectorize = None, dependency = None) -> RessourceDecorator:
        return self
    
    def return_value(self, run_output = "return", auto_save_on = None, loaders = None, group=None, unload = None, dependency = None, dtype = None) -> RessourceDecorator:
        return self
    
    def run(self, run_restrictions=None) -> RessourceDecorator :
        return self

    def __call__(self, f):
        @functools.wraps(f)
        def new_f(*args, **kwargs):
            s = inspect.signature(f).bind(*args, **kwargs)
            s.apply_defaults()
            return BasicRessource(f, s.arguments, [StorageSolution(DictLoader(), DictStorage(), "Dict")])
        return Proxy(new_f)
    