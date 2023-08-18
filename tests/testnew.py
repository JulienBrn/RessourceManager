from RessourceManager.new_ressources import RessourceDecorator

@RessourceDecorator()
def f(a, b):
    return a+b


r = f.declare(b=1, a=2)
r2 = f.declare(r, b=3)
print(r.identifier, r2.identifier)