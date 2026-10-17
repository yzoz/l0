import math

def logistic(x, k=1.0, x0=0.0):
    """
    x0 — точка, где значение равно 0.5
    k — крутизна
    """
    return 1 / (1 + math.exp(-k * (x - x0)))

def sigmoid(x, alpha, beta):
    """
    α — чем больше, тем быстрее уходит от 0
    β — чем меньше, тем медленнее приближается к 1
    """
    return 1 - (1 + alpha * x) ** (-beta)

alpha = 0.001
beta  = 0.25

'''for x in range(1, 10000, 100):
    print(sigmoid(x, alpha, beta), x)'''

x0 = 5000
k = 0.00001

for x in range(1, 100000, 1000):
    print(logistic(x, k, x0), x)