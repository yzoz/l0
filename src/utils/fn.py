alpha = 0.033
beta = 0.33


def transfer_score(x, alpha, beta):
    """
    α — чем больше, тем быстрее уходит от 0
    β — чем меньше, тем медленнее приближается к 1
    """
    return 1 - (1 + alpha * x) ** (-beta)
