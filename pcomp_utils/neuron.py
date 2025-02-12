import numpy as np
from pcomp.activation_functions import ACTIVATIONS, relu

class Neuron:
    def __init__(self, weights, bias, activation, is_final_layer=False):
        self.weights = np.array(weights)
        self.bias = np.array(bias)
        self.activation_func = None if is_final_layer else ACTIVATIONS.get(activation, relu)
        self.is_final_layer = is_final_layer
    
    def forward(self, inputs):
        z = np.dot(self.weights, inputs) + self.bias
        return z if self.is_final_layer else self.activation_func(z)