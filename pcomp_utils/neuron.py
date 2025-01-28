from pcomp.activation_functions import activation_fns

class Neuron:
    def __init__(self, neuron_id, weights, bias, activation):
        self.neuron_id = neuron_id  # Unique ID for this neuron
        self.weights = weights
        self.bias = bias
        self.activation = activation
        self.activated_by = []  # List of neuron IDs that activate this neuron
        self.activates = []     # List of neuron IDs that this neuron activates

    def activate(self, inputs):
        """
        Activate the neuron based on its inputs, weights, and bias.
        Apply the specified activation function to the weighted sum.
        """
        z = sum(w * i for w, i in zip(self.weights, inputs)) + self.bias
        return self.apply_activation(z)

    def apply_activation(self, z):
        """
        Apply the activation function to the input value (z).
        """
        if self.activation in activation_fns.keys():
            return activation_fns[self.activation](z)
        else:
            return z  # Identity function if no activation is defined