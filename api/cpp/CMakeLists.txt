project(acquire-zarr-cpp-api)

set(tgt acquire-zarr-cpp)
set(ACQUIRE_API_INCLUDE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/include/acquire-zarr/acquire-zarr.hh)

add_library(${tgt}
        ${ACQUIRE_API_INCLUDE_FILE}
        src/acquire-zarr-writer.cpp
        src/acquire-zarr-writer-impl.hh
        src/acquire-zarr-writer-impl.cpp
)


target_include_directories(${tgt} PRIVATE
  $<BUILD_INTERFACE: 
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
        ${tgt}_inc
  $<INSTALL_INTERFACE: include> 
)

target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        acquire-driver-zarr-obj
        blosc_static
        nlohmann_json::nlohmann_json
        miniocpp::miniocpp
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        PUBLIC_HEADER ${ACQUIRE_API_INCLUDE_FILE}
)

install(TARGETS ${tgt} 
        LIBRARY DESTINATION api/lib
        PUBLIC_HEADER DESTINATION api/include/acquire-zarr
)

option(ACQUIRE_BUILD_API_CPP_EXAMPLES "Build C++ API examples" OFF)
if(ACQUIRE_BUILD_API_CPP_EXAMPLES)
    add_subdirectory(examples)
endif()