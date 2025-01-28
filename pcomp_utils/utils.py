from pcomp.neuron import Neuron
from pcomp.layer import Layer

def build_layer(layer_data, neuron_start_id, previous_layer=None):
    """
    Construct a Layer object from the model data for a specific layer.
    Establish relationships (activated_by and activates) between neurons in
    the current and previous layers.
    """
    neurons = []
    for idx, (weights, bias) in enumerate(zip(layer_data["weights"], layer_data["biases"])):
        neuron_id = neuron_start_id + idx
        activation = layer_data["activation"]
        neuron = Neuron(neuron_id, weights, bias, activation)
        neurons.append(neuron)

    # If there is a previous layer, establish relationships
    if previous_layer:
        for prev_neuron in previous_layer.neurons:
            for curr_neuron in neurons:
                prev_neuron.activates.append(curr_neuron.neuron_id)
                curr_neuron.activated_by.append(prev_neuron.neuron_id)

    return Layer(neurons), neuron_start_id + len(neurons)