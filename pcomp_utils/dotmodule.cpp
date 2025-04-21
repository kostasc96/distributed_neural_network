// dotmodule.cpp
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <immintrin.h>
#include <cstddef>
#include <cmath>
#include <algorithm>
#include <omp.h>

namespace py = pybind11;

// ––––– AVX2‑accelerated dot+bias kernel (single thread) –––––
static inline double dot_avx2(const double* __restrict a,
                              const double* __restrict b,
                              std::size_t n,
                              double bias)
{
    std::size_t i = 0;
    double sum = bias;
#ifdef __AVX2__
    __m256d vsum = _mm256_setzero_pd();
    for (; i + 3 < n; i += 4) {
        __m256d va = _mm256_loadu_pd(a + i);
        __m256d vb = _mm256_loadu_pd(b + i);
        vsum = _mm256_fmadd_pd(va, vb, vsum);
    }
    // horizontal reduce
    __m128d low  = _mm256_castpd256_pd128(vsum);
    __m128d high = _mm256_extractf128_pd(vsum, 1);
    low = _mm_add_pd(low, high);
    low = _mm_hadd_pd(low, low);
    sum += _mm_cvtsd_f64(low);
#endif
    for (; i < n; ++i)
        sum += a[i] * b[i];
    return sum;
}

// ––––– OpenMP wrapper –––––
static double dot_parallel(const double* a,
                           const double* b,
                           std::size_t n,
                           double bias)
{
    double total = bias;
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int nth = omp_get_num_threads();
        std::size_t chunk = (n + nth - 1) / nth;
        std::size_t start = tid * chunk;
        std::size_t end   = std::min(start + chunk, n);
        if (start < end) {
            double local = dot_avx2(a + start, b + start, end - start, 0.0);
            #pragma omp atomic
            total += local;
        }
    }
    return total;
}

// ––––– Built‑in activations –––––
static double activate(double x, const std::string &act, double *scratch, std::size_t /*n*/) {
    if (act == "relu") {
        return x > 0.0 ? x : 0.0;
    }
    else if (act == "softmax") {
        // for a *single scalar* softmax(x) = 1.0
        // (i.e. exp(x–max)/∑exp = exp(0)/exp(0) = 1)
        return 1.0;
    }
    // "none" or unknown → identity
    return x;
}

// ––––– pybind11 entry point –––––
py::float_ dot_with_bias(py::array_t<double, py::array::c_style | py::array::forcecast> a,
                         py::array_t<double, py::array::c_style | py::array::forcecast> b,
                         double bias = 0.0,
                         const std::string &activation = "none",
                         bool parallel = false)
{
    auto A = a.unchecked<1>();
    auto B = b.unchecked<1>();
    if (A.shape(0) != B.shape(0))
        throw std::runtime_error("Input vectors must have the same length");

    // compute dot
    double out = parallel
        ? dot_parallel(A.data(0), B.data(0), A.shape(0), bias)
        : dot_avx2  (A.data(0), B.data(0), A.shape(0), bias);

    // apply activation exactly once
    double activated = activate(out, activation, nullptr, 0);
    return py::float_(activated);
}

PYBIND11_MODULE(dotmodule, m) {
    m.doc() = "dot(a, b) + bias with AVX2+OpenMP and built‑in ReLU/Softmax";
    m.def("dot_with_bias",
          &dot_with_bias,
          py::arg("a"), py::arg("b"),
          py::arg("bias")      = 0.0,
          py::arg("activation")= "none",
          py::arg("parallel")  = false,
          R"pbdoc(
Compute dot(a, b) + bias, *optionally* in parallel, and apply one of:
  – "none"    : identity
  – "relu"    : max(0, x)
  – "softmax" : returns 1.0 for a single scalar
)pbdoc");
}
