class NeuronsAccumulator:
    __slots__ = ('outputs', 'completed')
    def __init__(self, neuron_count):
        self.outputs = [None] * neuron_count
        self.completed = 0
