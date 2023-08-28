from RessourceManager.new_ressources import RessourceDecorator
from RessourceManager.lifting import ListLifting
from RessourceManager.storage import ReadableWriter
import time, tqdm, logging, beautifullogger

logger = logging.getLogger(__name__)
beautifullogger.setup()

print("f(a,b) adds a and b if a!=0 and raises an exception if a=0")

@RessourceDecorator()
def f(a, b):
    if a == 0:
        raise ValueError("a=0")
    return a+b

print("g(s, l) adds s and the sum of the list l")

@RessourceDecorator().add_writers(ReadableWriter())
def g(s, l, tqdm: tqdm.tqdm ):
    v = s
    tqdm.total = tqdm.total+len(l) if not tqdm.total is None else len(l)
    for i in l:
        time.sleep(0.01)
        v+=i
        tqdm.update()
    return v

print("h(s) returns s after sleeping s/10 secs")
@RessourceDecorator()
def h(s, tqdm: tqdm.tqdm ):
    tqdm.total = tqdm.total+s if not tqdm.total is None else s
    for i in range(s):
        time.sleep(0.1)
        tqdm.update()
    return s

print("\n\n")
try:
    r1 = f.declare(b=1, a=2)
    r2 = f.params("a", dependency="Value").declare(r1, b=3)
    r3 = f.declare(a=3, b=3)
    r4 = f.declare(a=0, b=3)
    r5 = f.params("a", vectorized = True).declare(a=[1,2], b = 3)
    r6 = f.params("a", exception="PassAsValue").declare(a=r4, b=1)
    r7 = f.params(lifting=ListLifting()).declare([r1, r2], r5)
    r8 = f.declare(f.declare(-1, 1), 2)
    r9 = f.declare(f.declare(a=1, b = f.declare(f.declare(1, -1), f.declare(1, 1))), 2)
    r10 = g.declare(g.declare(0, [1,2,3,4]*40), [6,7,8]*30)
    r11 = h.declare(50)
    # print(r1.identifier, r2.identifier, r7.identifier)
    # print(r2.result())
    excepts = []
    l = [r1, r2, r3, r4, r5[0], r5[1], r6, r7, r8, r9, r10, r11, r11]
    results = [r.result(exception=excepts, progress=tqdm.tqdm) for r  in l]
    print("\n".join([f"{r.identifier[0:50]}{'...)' if len(r.identifier) > 49 else ''} = {res}" for r, res in zip(l, results)])) 
    if excepts !=[]:
        # print("R9"+"\n".join([str(x) for x in r9.log]))
        raise ExceptionGroup("Exceptions while computing results", excepts)
    # print("R2"+"\n".join([str(x) for x in r2.log]))
    # print("R1"+"\n".join([str(x) for x in r1.log]))
    # print("R3"+"\n".join([str(x) for x in r3.log]))
except Exception:
    r = input("\n\nExceptions occured, do you want to display them? Y/n")
    print("\n\n")
    if not "n" in r.lower():
        raise
finally:
    r = input("\n\nDo you want to view ressource logs? N/y")
    print("\n\n")
    if "y" in r.lower():
        for rec in l:
            logtext = '\n'.join([str(x) for x in rec.log])
            print(f"Log for {rec.identifier[0:50]}{'...)' if len(rec.identifier) > 49 else ''}\n{logtext}\n\n")


# print("\n".join([str(x) for x in r2.log]))