#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include "acquire-zarr/acquire-zarr.hh"
#include <iostream>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)
namespace py = pybind11;


/**
 * @brief PyAcquireZarrWriter is a python interface for AcquireZarrWriter 
 * which is written in C++.  Any pybind11-spcific code should be written here, 
 * and everything else should pass through.
 * 
 */
class PyAcquireZarrWriter : public AcquireZarrWriter
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
        Acquire Zarr Writer Python API
        -----------------------

        .. currentmodule:: acquire_zarr

        .. autosummary::
           :toctree: _generate

           append
    )pbdoc";

    py::class_<PyAcquireZarrWriter>(m, "AcquireZarrWriter")
        .def(py::init<>())
        .def("append", &PyAcquireZarrWriter::append)
        //.def("open", &PyAcquireZarrWriter::open);
        .def_property("shape", &PyAcquireZarrWriter::get_shape, &PyAcquireZarrWriter::set_shape);

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}