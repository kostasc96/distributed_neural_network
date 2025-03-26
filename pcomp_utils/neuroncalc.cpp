#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <eigen3/Eigen/Dense>
#include <algorithm>
#include <string>

namespace py = pybind11;
using Eigen::VectorXd;
using Eigen::MatrixXd;

// ReLU activation
double relu(double x) {
    return x > 0.0 ? x : 0.0;
}

// Per-neuron computation for one row
py::array_t<double> compute_neuron_output_batch(
    py::array_t<double> input_arr,
    py::array_t<double> weight_arr,
    double bias,
    const std::string& activation) {

    auto input = input_arr.unchecked<2>(); // batch_size x input_dim
    auto weight = weight_arr.unchecked<1>();

    size_t batch_size = input.shape(0);
    size_t input_dim = input.shape(1);

    std::vector<double> outputs;
    outputs.reserve(batch_size);

    for (size_t b = 0; b < batch_size; b++) {
        double z = 0.0;
        for (size_t i = 0; i < input_dim; i++) {
            z += input(b, i) * weight(i);
        }
        z += bias;

        if (activation == "relu") {
            z = relu(z);
        }
        outputs.push_back(z);
    }

    return py::array_t<double>(batch_size, outputs.data());
}

PYBIND11_MODULE(neuroncalc, m) {
    m.def("compute_neuron_output_batch", &compute_neuron_output_batch, "Compute per-neuron output for all input rows");
}
