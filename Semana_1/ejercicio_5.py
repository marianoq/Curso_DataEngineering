b = []
for i in range(1, 101, 1):
    if i % 2 == 0:
        b.append(0)
    else:
        b.append(i)
print('La suma de los numeros impares entre 0 y 100 es: ',sum(b))

suma = 0
for i in range(1, 101, 2):
    suma += 1
print('La suma de los numeros impares entre 0 y 100 es: ',sum(b))
