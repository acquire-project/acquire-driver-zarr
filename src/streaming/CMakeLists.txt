set(tgt acquire-zarr)

add_library(${tgt}
        macros.hh
        acquire.zarr.cpp
        zarr.dimension.hh
        zarr.dimension.cpp
        zarr.stream.hh
        zarr.stream.cpp
        zarr.common.hh
        zarr.common.cpp
)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/logger>
)

target_link_libraries(${tgt} PRIVATE
        acquire-logger
        blosc_static
        miniocpp::miniocpp
)

target_compile_definitions(${tgt} PRIVATE
        "ACQUIRE_ZARR_API_VERSION=\"0.0.1\""
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt}
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

# Install public header files
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION include
)