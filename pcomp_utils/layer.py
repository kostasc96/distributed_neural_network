class Layer:
    def __init__(self, neurons):
        self.neurons = neurons  # List of neurons in this layer

    def forward(self, inputs):
        """
        Perform forward propagation through this layer.
        """
        return [neuron.activate(inputs) for neuron in self.neurons]