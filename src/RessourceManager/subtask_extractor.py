from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, TypeVar, Generic, Sequence

T = TypeVar('T')
SubtaskExtractor = Callable[[Any], Tuple[Dict[Any, T], Callable[[Dict[Any, Any]], Any]]]
DynamicSubtaskExtractorPreprocess = Callable[[Any], Any]

# class EmbeddingRetriever(Generic[T]):
#     def __init__(self, cls):
#         self.cls = cls
#     def no_embedding_retriever(self, t: Any) -> Tuple[Dict[str, T], Callable[[Dict[str, Any]], Any]]:
#         if isinstance(t, self.cls):
#             return {"": t}, lambda d: d[""]
#         else:
#             return {}, lambda d: t
        
#     def sequence_embedding_retriever(self, t: Sequence[Any]) -> Tuple[Dict[int, T], Callable[[Dict[int, Any]], Any]]:
#         items = {i: val for i,val in enumerate(t) if isinstance(val, self.cls)}
#         def reconstruct(d: Dict[int, Any]):
#             r = t.copy()
#             r[*d.keys()] = d.values()
#             return r
#         return items, reconstruct
    
#     def dict_embedding_retriever(self, t: Dict[Any, Any]) -> Tuple[Dict[Any, T], Callable[[Dict[Any, Any]], Any]]:
#         items = {k: val for k,val in t.items() if isinstance(val, self.cls)}
#         def reconstruct(d: Dict[int, Any]):
#             r = t.copy()
#             r[*d.keys()] = d.values()
#             return r
#         return items, reconstruct
    
def make_basic_subtask_extractor(depth: int = 0) -> SubtaskExtractor:
    from RessourceManager.task import Task
    cls = Task
    def subtask_extractor(mdepth, t):
        if isinstance(t, dict) and mdepth != 0:
            items = {str(k): v for k, v in t.items()}
        elif (isinstance(t, list) or isinstance(t, tuple))  and mdepth!=0:
            items = {str(i):v for i, v in enumerate(t)}
        else:
            if isinstance(t, cls):
                return {"": t}, lambda d: d[""]
            else:
                return {}, lambda d: t
        if len([1 for k in items.keys() if "$" in k]) >0:
            raise ValueError("$ symbol not allowed in keys")
        if len(t) != len(items):
            raise ValueError("Key Duplication...")
        # input(str(items))
        items = {k: subtask_extractor(mdepth-1, val) for k, val in items.items()}
        
        
        d = {f"{k}${kc}":(vc, rec) for k, (d, rec) in items.items() for kc, vc in d.items() }
        dout = {k:v[0] for k,v in d.items()}
        def reconstruct(d_in: Dict[str, Any]):
            new_items = {k:{} for k in items.keys()}
            for k in d_in.keys():
                l = k.split("$")
                x = l[0]
                rest = l[1:]
                new_items[x]["$".join(rest)] = d_in[k]
            new_items = {k:items[k][1](new_items[k]) for k in new_items.keys()}
            return new_items
        return dout, reconstruct
    return lambda t: subtask_extractor(depth, t)