import math

activation_fns = {}
activation_fn = lambda f: activation_fns.setdefault(f.__name__, f)

@activation_fn
def relu(x):
    return max(0, x)

@activation_fn
def sigmoid(x):
    return 1 / (1 + math.exp(-x))

@activation_fn
def tanh(x):
    return math.tanh(x)