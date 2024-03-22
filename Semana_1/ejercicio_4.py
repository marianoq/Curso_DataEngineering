a = list(range(5,21,1))
b = list(range(10,21,1))
c=[]
for numero in b:
    if numero in a:
        c.append(numero)
print([*set(c)])