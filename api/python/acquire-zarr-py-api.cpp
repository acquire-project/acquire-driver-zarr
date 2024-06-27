#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include "acquire-zarr/acquire-zarr.hh"
#include <iostream>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)
namespace py = pybind11;

class PyAcquireZarrWriter : private AcquireZarrWriter
{
public:
    using AcquireZarrWriter::AcquireZarrWriter;
    PyAcquireZarrWriter() = default;
    ~PyAcquireZarrWriter() = default;
    
    void append(py::array image_data)
    {
        auto buf = image_data.request();
        uint8_t *ptr = (uint8_t*)buf.ptr;
        AcquireZarrWriter::append(ptr, buf.itemsize * buf.size);

    }
};

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

    py::class_<PyAcquireZarrWriter>(m, "AcquireZarrWriter")
        .def(py::init<>())
        .def("append", &PyAcquireZarrWriter::append);


#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}