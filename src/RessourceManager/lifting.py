from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools
from RessourceManager.new_ressources import RessourceData


class Lifting:
    def deconstruct(self, obj) -> List[RessourceData]:pass
    def reconstruct(self, objs: List[Any]) -> Any: pass

class NoLifting(Lifting):
    def deconstruct(self, obj) -> List[RessourceData]:
        self.obj = obj
        if isinstance(obj, RessourceData):
            return [obj]
        else:
            return []
    def reconstruct(self, objs: List[Any]) -> Any: 
        if isinstance(self.obj, RessourceData):
            return objs[0]
        else:
            return self.obj
        
class ListLifting(Lifting):
    def deconstruct(self, obj) -> List[RessourceData]:
        self.obj = obj
        if isinstance(obj, RessourceData):
            return [obj]
        elif isinstance(obj, list):
            res=[]
            for x in obj:
                if isinstance(x, RessourceData):
                    res.append(x)
            return res
        else:
            return []
    def reconstruct(self, objs: List[Any]) -> Any: 
        if isinstance(self.obj, RessourceData):
            return objs[0]
        elif isinstance(self.obj, list):
            res=[]
            ind=0
            for x in self.obj:
                if isinstance(x, RessourceData):
                    res.append(objs[ind])
                    ind+=1
                else:
                    res.append(x)
            return res
        else:
            return self.obj