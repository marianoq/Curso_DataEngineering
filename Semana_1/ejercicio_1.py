while True:
    a = int(input('Ingrese un numero impar:'))
    if a % 2==0:
        print(f'El numero {a} no es impar, ingrese un numero impar. ')
    else:
        print(f'El numero {a} es impar, ciclo finalizado.')
        break