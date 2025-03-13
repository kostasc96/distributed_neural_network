import numpy as np

def relu(x):
    return np.maximum(0, x)

def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

ACTIVATIONS = {
    "relu": relu,
    "softmax": softmax
}