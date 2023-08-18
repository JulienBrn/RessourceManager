from RessourceManager.new_ressources import RessourceDecorator

@RessourceDecorator().params(dependency="Value")
def f(a, b):
    return a+b


r = f.declare(b=1, a=2)
r2 = f.declare(r, b=3)
print(r.identifier, r2.identifier)
print(r2.result())
print(r.result(), r2.result())
print("\n".join([str(x) for x in r2.log]))