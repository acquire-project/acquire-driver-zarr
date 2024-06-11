set(ACQUIRE_BUILD_API_CPP OFF CACHE BOOL "Build the stand-alone C++ API")

if(ACQUIRE_BUILD_API_CPP)
    add_subdirectory(cpp)
endif()
