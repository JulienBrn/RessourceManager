import asyncio, shutil, pathlib
if pathlib.Path(".pickled_cache").exists():
    shutil.rmtree(".pickled_cache")

from RessourceManager.memory_ressource import RessourceDecorator, d, pickled_disk_storage, raw_memory_storage
import pandas as pd
import time

from os.path import join, abspath
from traceback import extract_tb, format_list, format_exception_only




def trimmedexceptions(type, value, tb, pylibdir=None, lev=None):
    import traceback
    s = traceback.format_exception(type, value, tb)
    # s = t.format()
    print("\n".join([x for x in ("\n".join([x for x  in s])).split("\n") ])) #if "memory_ressource" not in x

import sys
sys.excepthook=trimmedexceptions


class DenominatorError(Exception):
    pass


def check_denom(b):
    if b==0:
        raise DenominatorError("denominator error")
@RessourceDecorator("divide")
def divide(a, b):
    print(f"divide called with a={a}, b={b}")
    check_denom(b)
    return a / b

@RessourceDecorator("divide2")
def divide2(a, b, debug=True):
    print(f"divide2 called with a={a}, b={b}, debug={debug}")
    return a//b

@RessourceDecorator("divide3").param("debug", ignore=True)
def divide3(a, b, debug=True):
    print(f"divide2 called with a={a}, b={b}, debug={debug}")
    return a//b

@RessourceDecorator("divide4").param("debug", ignore=True).param("b", parameter=raw_memory_storage)
def divide4(a, b, debug=True):
    print(f"b location {b}")
    b = b[0].load(b[1])
    print(f"divide2 called with a={a}, b={b}, debug={debug}")
    return a//b

data = pd.DataFrame([[1, 0], [1, 1], [2, 3], [0,1], [1, 1], [1,1], [0, 0], [1, 0]], columns=["a", "b"])
interupt_fast = False
# data=data.loc[data["b"]!=0]

rec_df= pd.DataFrame()
rec_df["rec1"] = data.apply(lambda row: divide.declare(row["a"], row["b"]), axis=1)

rec_df["rec2"] = divide2.vectorized("a").declare(data["a"], 1)
rec_df["rec3"] = divide2.vectorized("a").declare(data["a"], b=1)
rec_df["rec4"] = divide2.vectorized("a").declare(b=2, a = data["a"])
rec_df["rec5"] = divide2.vectorized("a", "b").declare(b=data["b"], a = data["a"])
rec_df["rec6"] = divide2.vectorized("a", "b").declare(b=data["b"], a = data["a"], debug=True)
rec_df["rec7"] = divide2.vectorized("a", "b").declare(b=data["b"], a = data["a"], debug=False)

rec_df["rec8"] = divide3.vectorized("a", "b").declare(b=data["b"], a = data["a"])
rec_df["rec9"] = divide3.vectorized("a", "b").declare(b=data["b"], a = data["a"], debug=True)
rec_df["rec10"] = divide3.vectorized("a", "b").declare(b=data["b"], a = data["a"], debug=False)
rec_df["rec11"] = divide3.vectorized("a", "b").declare(b=rec_df["rec10"], a = data["a"], debug=False)
rec_df["rec13"] = divide.vectorized("a", "b").declare(b=rec_df["rec1"], a = data["a"])

rec_df["rec12"] = divide4.vectorized("a", "b").declare(b=rec_df["rec10"], a = data["a"], debug=False)

id_df = pd.DataFrame()
for col in rec_df.columns:
    id_df[col+".id"] = rec_df[col].apply(lambda x:x.id)


result_df = pd.DataFrame()

def my_get(x, exceptions):
    if interupt_fast:
        return x.get()
    try:
        return x.get()
    except BaseException as e:
        exceptions.append(e)
        return None
try:
    exceptions = []
    for col in rec_df.columns:
        # if col is "rec1":
        #     continue
        try:
            col_exceptions = []
            result_df[col+".res"] = rec_df[col].apply(lambda x: my_get(x, col_exceptions))
            if not col_exceptions==[]:
                raise ExceptionGroup(f"Errors while computing column {col}", col_exceptions)
        except BaseException as e:
            if interupt_fast:
                raise e
            exceptions.append(e)
    if not exceptions==[]:
        raise ExceptionGroup(f"Errors while computing result_df", exceptions)

except * DenominatorError as eg:
    raise
finally:
    # print("\n".join(d.keys()))
    # print(id_df)
    print(result_df)

# data["rec"] = 
# data["recid"] = data["rec"].apply(lambda x:x.id)
# data["res"] = data["rec"].apply(lambda x:x.get())

# # print(divide2.declare(data["a"], 1)[0].get())

# data["rec2"] = divide2.vectorized("a").declare(data["a"], 1)
# data["recid2"] = data["rec2"].apply(lambda x:x.id)
# data["res2"] = data["rec2"].apply(lambda x:x.get())

# data["rec3"] = divide2.vectorized("a").declare(data["a"], b=1)
# data["recid3"] = data["rec3"].apply(lambda x:x.id)
# data["res3"] = data["rec3"].apply(lambda x:x.get())

# data["rec4"] = divide2.vectorized("a").declare(b=2, a = data["a"])
# data["recid4"] = data["rec4"].apply(lambda x:x.id)
# data["res4"] = data["rec4"].apply(lambda x:x.get())

# data["rec5"] = divide2.vectorized("a", "b").declare(b=data["b"], a = data["a"])
# data["recid5"] = data["rec5"].apply(lambda x:x.id)
# data["res5"] = data["rec5"].apply(lambda x:x.get())

# data["rec6"] = divide2.vectorized("a", "b").declare(b=data["b"], a = data["a"], debug=True)
# data["recid6"] = data["rec6"].apply(lambda x:x.id)
# data["res6"] = data["rec6"].apply(lambda x:x.get())



# print(data)