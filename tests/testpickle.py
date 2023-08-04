import pickle, traceback, sys
import logging
import beautifullogger
import tblib
from tblib import pickling_support


logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)
beautifullogger.setup()

def buggy(x):
    raise ValueError("my error")

def f(x):
    if x < 10:
        return buggy(x)
    else:
        return x+2



class DelayedException(Exception):
    def __init__(self, s):
        super().__init__(s)



for val, method in [
    (0, "no_cache"), (100, "no_cache"), #direct function call (what we want to imitate)
    (0, "memory_cached"), (100, "memory_cached"), #reraising later without saving/loading (currently good result)
    (0, "pickled_cache"), (100, "pickled_cache") #reraising later with saving/loading in between
]:
    print("\n\n")
    try:
        logger.info(f"val = {val}, method={method}")
        if method=="no_cache":
            res = f(val)
        else:
            try:
                res = f(val)
            except BaseException as e:
                try:
                    #Not very readable technique to append this exception in the exception stack.
                    #I'm very open to something better, but it is important we do not change the type of the exception
                    raise DelayedException(f"Error in {f.__name__}({val})") 
                except DelayedException as d:
                    try:
                        raise e from d
                    except BaseException as final:
                        res = final

            if method=="pickled_cache":
                pickling_support.install()
                res = pickle.dumps(res)
                res = pickle.loads(res)

            if isinstance(res, BaseException):
                raise res

        logger.info(f"ok, res = {res}")
    except:
        logger.error(traceback.format_exc())



