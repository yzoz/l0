alpha = 0.0005
beta  = 0.5

def transfer_score(x, alpha, beta):
    """
    α — чем больше, тем быстрее уходит от 0
    β — чем меньше, тем медленнее приближается к 1
    """
    return 1 - (1 + alpha * x) ** (-beta)

'''
пусть пока будет здесь

def logistic(x, k=1.0, x0=0.0):
    """
    x0 — точка, где значение равно 0.5
    k — крутизна
    """
    return 1 / (1 + math.exp(-k * (x - x0)))

for x in range(1, 100000, 1000):
    print(sigmoid(x, alpha, beta), x)

x0 = 5000
k = 0.00001

for x in range(1, 100000, 1000):
    print(logistic(x, k, x0), x)
'''