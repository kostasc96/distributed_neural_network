from base64 import b64decode

cdef class LayerCoordinatorMessage:
    cdef public int neuron_id
    cdef public int image_id
    cdef public double output

    def __init__(self, int neuron_id, int image_id, double output):
        self.neuron_id = neuron_id
        self.image_id = image_id
        self.output = output


def parse_layer_coordinator_message(str message):
    cdef list parts = message.split('|')
    cdef int neuron_id = int(parts[0])
    cdef int image_id = int(parts[1])
    cdef double output = float(parts[2])
    return LayerCoordinatorMessage(neuron_id, image_id, output)


cdef class LayerMessage:
    cdef public str layer
    cdef public int image_id

    def __init__(self, str layer, int image_id):
        self.layer = layer
        self.image_id = image_id


def parse_layer_message(str message):
    cdef list parts = message.split('|')
    cdef str layer = parts[0]
    cdef int image_id = int(parts[1])
    return LayerMessage(layer, image_id)
