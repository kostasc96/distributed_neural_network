import numpy as np
cimport numpy as np

def dict_to_vector(dict outputs, int neuron_count):
    cdef np.ndarray[np.float64_t, ndim=1] vec = np.empty(neuron_count, dtype=np.float64)
    cdef int i
    for i in range(neuron_count):
        vec[i] = float(outputs[str(i)])
    return vec
