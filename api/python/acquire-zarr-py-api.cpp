#include <pybind11/pybind11.h>
#include "acquire-zarr/acquire-zarr.hh"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

int add(int i, int j) {
    return i + j;
}

namespace py = pybind11;

PYBIND11_MODULE(acquire_zarr, m) {
    m.doc() = R"pbdoc(
        Pybind11 example plugin
        -----------------------

        .. currentmodule:: acquire_zarr

        .. autosummary::
           :toctree: _generate

           add
           subtract
    )pbdoc";
    py::class_<AcquireZarrWriter>(m, "AcquireZarrWriter")
        .def("append", &AcquireZarrWriter::append);
    // m.def("add", &add, R"pbdoc(
    //     Add two numbers

    //     Some other explanation about the add function.
    // )pbdoc");

    // m.def("subtract", [](int i, int j) { return i - j; }, R"pbdoc(
    //     Subtract two numbers

    //     Some other explanation about the subtract function.
    // )pbdoc");

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}