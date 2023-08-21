from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict
import pandas as pd, tqdm, numpy as np
import logging, hashlib, functools

RessourceData = None


class Lifting:
    def deconstruct(self, obj) -> List[RessourceData]:pass
    # def reconstruct(self, objs: List[Any]) -> Any: pass

class NoLifting(Lifting):
    def deconstruct(self, obj) -> List[RessourceData]:
        if isinstance(obj, RessourceData):
            return [obj], lambda x: x[0]
        else:
            return [], lambda x:obj
        # def reconstruct(self, objs: List[Any]) -> Any: 
        #     if isinstance(obj, RessourceData):
        #         return objs[0]
        #     else:
        #         return self.obj
        
class ListLifting(Lifting):
    def deconstruct(self, obj) -> List[RessourceData]:
        # self.obj = obj
        if isinstance(obj, RessourceData):
            return [obj], lambda x: x[0]
        elif isinstance(obj, list):
            res=[]
            for x in obj:
                if isinstance(x, RessourceData):
                    res.append(x)
            def refill(l):
                i=0
                mres=[]
                for x in obj:
                    if isinstance(x, RessourceData):
                        mres.append(l[i])
                        i+=1
                return mres
            return res, refill
        else:
            return [], lambda e: obj
    # def reconstruct(self, objs: List[Any]) -> Any: 
    #     if isinstance(self.obj, RessourceData):
    #         return objs[0]
    #     elif isinstance(self.obj, list):
    #         res=[]
    #         ind=0
    #         for x in self.obj:
    #             if isinstance(x, RessourceData):
    #                 res.append(objs[ind])
    #                 ind+=1
    #             else:
    #                 res.append(x)
    #         return res
    #     else:
    #         return self.obj