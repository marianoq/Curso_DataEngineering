numeros=int(input(f'Introduce el numero de numeros a calcular la media:'))
lista=[]
for i in range(numeros):
    a=float(input(f'Introduce numero:'.format(i+1)))
    lista.append(a)
print(f'La media de los numeros ingresados {lista} es: {sum(lista)/len(lista)}')