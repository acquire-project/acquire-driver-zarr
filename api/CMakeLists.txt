set(ACQUIRE_BUILD_API_CPP OFF CACHE BOOL "Build the stand-alone C++ API")
set(ACQUIRE_BUILD_API_C OFF CACHE BOOL "Build the stand-alone C API (in development, not yet implemented)")
set(ACQUIRE_BUILD_API_PY OFF CACHE BOOL "Build the stand-alone Python API")

if(ACQUIRE_BUILD_API_C)
    set(ACQUIRE_BUILD_API_CPP ON CACHE BOOL "Build the stand-alone C++ API" FORCE)
    add_subdirectory(c)
endif()

if(ACQUIRE_BUILD_API_CPP)
    add_subdirectory(cpp)
endif()

if(ACQUIRE_BUILD_API_PY)
    set(ACQUIRE_BUILD_API_CPP ON CACHE BOOL "Build the stand-alone C++ API" FORCE)
    add_subdirectory(python)
endif()