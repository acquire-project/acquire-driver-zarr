set(ACQUIRE_BUILD_API_CPP OFF CACHE BOOL "Build the stand-alone C++ API")
set(ACQUIRE_BUILD_API_PY OFF CACHE BOOL "Build the stand-alone Python API")

# build the C++ API if requested, or as a dependency of the Python API
if(ACQUIRE_BUILD_API_CPP OR ACQUIRE_BUILD_API_PY)
    add_subdirectory(cpp)  #this must be added first, in order for the include paths to happen correctly.
endif()

# build the Python API if requested
if(ACQUIRE_BUILD_API_PY)
    add_subdirectory(python)
endif()
