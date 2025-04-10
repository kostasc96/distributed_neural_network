#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <cstring>  // for memcpy

namespace py = pybind11;

py::array_t<double> get_output_vector_cpp(py::dict hash_data, int neuron_count) {
    // Check if the dictionary has the expected number of items.
    if (py::len(hash_data) != neuron_count) {
        std::ostringstream oss;
        oss << "Output vector incomplete: " << py::len(hash_data)
            << " of " << neuron_count << " neurons written.";
        throw std::runtime_error(oss.str());
    }
    
    // Prepare a vector of doubles with the expected number of neurons.
    std::vector<double> vec(neuron_count, 0.0);
    
    // Iterate over each key-value pair in the dictionary.
    for (auto item : hash_data) {
        // Cast the key and value to std::string.
        std::string key = item.first.cast<std::string>();      // expects a byte string convertible to std::string
        std::string value_str = item.second.cast<std::string>(); // likewise for value
        
        int index;
        try {
            index = std::stoi(key);
        } catch (const std::exception& e) {
            throw std::runtime_error("Key conversion error for key: " + key);
        }
        
        if (index < 0 || index >= neuron_count) {
            throw std::runtime_error("Index out of range: " + key);
        }
        
        double value;
        try {
            value = std::stod(value_str);
        } catch (const std::exception& e) {
            throw std::runtime_error("Value conversion error for key: " + key + " with value: " + value_str);
        }
        
        vec[index] = value;
    }
    
    // Create a NumPy array of the correct size.
    auto result = py::array_t<double>(vec.size());
    
    // Get a pointer to the NumPy array's data.
    double* ptr = static_cast<double*>(result.request().ptr);
    
    // Copy the data from the vector into the NumPy array.
    std::memcpy(ptr, vec.data(), vec.size() * sizeof(double));
    
    return result;
}

PYBIND11_MODULE(fast_vector_cpp, m) {
    m.def("get_output_vector_cpp", &get_output_vector_cpp, "Convert raw hash data to numpy array");
}