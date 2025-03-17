from numba import njit

@njit
def compute_z(inputs, weights, bias):
    s = 0.0
    for i in range(len(inputs)):
        s += inputs[i] * weights[i]
    return s + bias

@njit
def relu_numba(x):
    return x if x > 0.0 else 0.0

@njit
def process_data_numba(inputs, weights, bias, isFinal):
    z = compute_z(inputs, weights, bias)
    if isFinal:
        return z
    else:
        return relu_numba(z)