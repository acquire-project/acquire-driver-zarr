add_subdirectory(3rdParty)

if (NOT TARGET acquire-core-logger)
    add_subdirectory(acquire-core-libs)
endif ()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        common.hh
        common.cpp
        writers/writer.hh
        writers/writer.cpp
        writers/chunk.writer.hh
        writers/chunk.writer.cpp
        writers/shard.writer.hh
        writers/shard.writer.cpp
        writers/blosc.compressor.hh
        writers/blosc.compressor.cpp
        zarr.hh
        zarr.cpp
        zarr.v2.hh
        zarr.v2.cpp
        zarr.v3.hh
        zarr.v3.cpp
        zarr.driver.c
)
target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        cblosc
        json
)
set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)
