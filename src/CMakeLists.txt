if (NOT TARGET acquire-core-logger)
    add_subdirectory(${CMAKE_SOURCE_DIR}/acquire-common/acquire-core-libs ${CMAKE_CURRENT_BINARY_DIR}/acquire-core-libs)
endif ()

if (NOT TARGET acquire-zarr)
    add_subdirectory(${CMAKE_SOURCE_DIR}/acquire-zarr ${CMAKE_CURRENT_BINARY_DIR}/acquire-zarr)
endif ()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        zarr.storage.hh
        zarr.storage.cpp
        zarr.driver.c
)

target_include_directories(${tgt} PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        acquire-logger
        acquire-zarr
        blosc_static
        nlohmann_json::nlohmann_json
        miniocpp::miniocpp
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)