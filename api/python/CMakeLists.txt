cmake_minimum_required(VERSION 3.5...3.27)
project(acquire-zarr-py)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 REQUIRED)

pybind11_add_module(acquire_zarr acquire-zarr-py-api.cpp)

target_include_directories(acquire_zarr PRIVATE ${acquire-zarr-cpp-api_SOURCE_DIR}/include) 
target_link_libraries(acquire_zarr PRIVATE acquire-zarr-cpp)

install(TARGETS acquire_zarr LIBRARY DESTINATION acquire_zarr)