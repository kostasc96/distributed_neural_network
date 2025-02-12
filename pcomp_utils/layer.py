import numpy as np
from pcomp.activation_functions import softmax

class Layer:
    def __init__(self, neurons, is_final_layer=False):
        self.neurons = neurons
        self.is_final_layer = is_final_layer
    
    def forward(self, inputs):
        outputs = np.array([neuron.forward(inputs) for neuron in self.neurons])
        return softmax(outputs) if self.is_final_layer else outputs