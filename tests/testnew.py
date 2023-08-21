from RessourceManager.new_ressources import RessourceDecorator

@RessourceDecorator()
def f(a, b):
    if a == 0:
        raise ValueError("a=0")
    return a+b

@RessourceDecorator().params()
def f(a, b):
    if a == 0:
        raise ValueError("a=0")
    return a+b




r1 = f.declare(b=1, a=2)
r2 = f.params("a", dependency="Value").declare(r1, b=3)
r3 = f.declare(a=3, b=3)
r4 = f.declare(a=0, b=3)
r5 = f.params("a", vectorized = True).declare(a=[1,2], b = 3)
print(r1.identifier, r2.identifier)
print(r2.result())
print(r1.result(), r2.result(), r3.result(), r5[0].result(), r5[1].result()) 
print("R2"+"\n".join([str(x) for x in r2.log]))
print("R1"+"\n".join([str(x) for x in r1.log]))
print("R3"+"\n".join([str(x) for x in r3.log]))
# print("\n".join([str(x) for x in r2.log]))