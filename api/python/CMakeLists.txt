cmake_minimum_required(VERSION 3.5...3.27)
project(acquire-zarr-py)

# Find pip-installed pybind11
execute_process(COMMAND python3 -m pybind11 --cmakedir
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
LIST(APPEND CMAKE_MODULE_PATH ${pybind11_DIR})

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 REQUIRED)

pybind11_add_module(acquire_zarr acquire-zarr-py-api.cpp)

target_include_directories(acquire_zarr PRIVATE ${acquire-zarr-cpp-api_SOURCE_DIR}/include) 
target_link_libraries(acquire_zarr PRIVATE acquire-zarr-cpp)

set_target_properties(acquire_zarr PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS acquire_zarr LIBRARY DESTINATION acquire_zarr)