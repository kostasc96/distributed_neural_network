import numpy as np

# Activation functions
def relu(x):
    return np.maximum(0, x)

def softmax(x):
    e_x = np.exp(x - np.max(x))  # Stability trick to prevent overflow
    return e_x / e_x.sum()

ACTIVATIONS = {
    "relu": relu,
    "softmax": softmax
}