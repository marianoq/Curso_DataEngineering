def contar(lista, elemento):
    contador = 0
    l=[]
    for x in list(lista):
        if x == elemento:
            l.append(x)    
    return l

lt = ['a','b','2','a','r','t','a','3','4','5','6','a','j','a','k','o','p']
x = 'a' 
contador=contar(lt, x)
print(len(contador))