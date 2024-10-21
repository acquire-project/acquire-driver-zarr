if (NOT TARGET acquire-core-logger)
    add_subdirectory(${CMAKE_SOURCE_DIR}/acquire-common/acquire-core-libs ${CMAKE_CURRENT_BINARY_DIR}/acquire-core-libs)
endif ()

if (NOT TARGET acquire-driver-zarr)
    add_subdirectory(${CMAKE_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/acquire-driver-zarr)
endif ()


set(tgt benchmark)
add_executable(${tgt}
        benchmark.cpp
)

target_include_directories(${tgt} PRIVATE
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
)

target_link_libraries(${tgt}
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-hal
        acquire-device-properties
        cblosc
        json
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

add_custom_target(copy-acquire-driver-zarr-for-benchmark
        COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:acquire-driver-zarr>
        $<TARGET_FILE_DIR:${tgt}>
        DEPENDS acquire-driver-zarr
        COMMENT "Copying acquire-driver-zarr to $<TARGET_FILE_DIR:${tgt}>"
)

add_dependencies(${tgt} copy-acquire-driver-zarr-for-benchmark)