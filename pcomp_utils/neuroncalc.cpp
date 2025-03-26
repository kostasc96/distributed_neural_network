#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <eigen3/Eigen/Dense>
#include <stdexcept>
#include <string>

namespace py = pybind11;

// ReLU activation function
double relu(double x) {
    return x > 0.0 ? x : 0.0;
}

py::array_t<double> compute_neuron_output_batch(
    py::array_t<double> input_arr,
    py::array_t<double> weight_arr,
    double bias,
    const std::string& activation) {

    // Request buffer info from numpy arrays
    auto buf_input = input_arr.request();
    auto buf_weight = weight_arr.request();

    // Validate dimensions
    if (buf_input.ndim != 2)
        throw std::runtime_error("Input array must be 2D");
    if (buf_weight.ndim != 1)
        throw std::runtime_error("Weight array must be 1D");

    size_t batch_size = buf_input.shape[0];
    size_t input_dim = buf_input.shape[1];

    if (buf_weight.shape[0] != input_dim)
        throw std::runtime_error("Weight dimension must match input dimension");

    // Map the input array to an Eigen matrix (using row-major storage as numpy arrays are row-major)
    double* ptr_input = static_cast<double*>(buf_input.ptr);
    Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>> input_mat(ptr_input, batch_size, input_dim);

    // Map the weight array to an Eigen vector
    double* ptr_weight = static_cast<double*>(buf_weight.ptr);
    Eigen::Map<Eigen::VectorXd> weight_vec(ptr_weight, input_dim);

    // Compute output using Eigen's matrix-vector multiplication with .noalias() for efficiency
    Eigen::VectorXd output(batch_size);
    output.noalias() = input_mat * weight_vec;
    output.array() += bias;

    // Apply activation if requested
    if (activation == "relu") {
        output = output.unaryExpr([](double v) { return relu(v); });
    }

    // Allocate numpy array for the result and copy the output data
    py::array_t<double> result(batch_size);
    auto buf_result = result.request();
    double* ptr_result = static_cast<double*>(buf_result.ptr);
    Eigen::Map<Eigen::VectorXd>(ptr_result, batch_size) = output;

    return result;
}

PYBIND11_MODULE(neuroncalc, m) {
    m.doc() = "Neuron calculation module using Eigen and Pybind11";
    m.def("compute_neuron_output_batch", &compute_neuron_output_batch,
          "Compute per-neuron output for all input rows");
}
