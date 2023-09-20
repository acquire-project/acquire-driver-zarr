add_subdirectory(3rdParty)

if (NOT TARGET acquire-core-logger)
    add_subdirectory(acquire-core-libs)
endif ()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        prelude.h
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
        czar.hh
        czar.cpp
        czar.v2.hh
        czar.v2.cpp
        czar.v3.hh
        czar.v3.cpp
#        tiled.frame.hh
#        tiled.frame.cpp
#        chunk.writer.hh
#        chunk.writer.cpp
#        frame.scaler.hh
#        frame.scaler.cpp
#        zarr.hh
#        zarr.cpp
#        zarr.encoder.hh
#        zarr.encoder.cpp
#        zarr.raw.hh
#        zarr.raw.cpp
#        zarr.blosc.hh
#        zarr.blosc.cpp
        zarr.driver.c
        3rdParty/crc32c/inc/crc32c.h
)
target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        cblosc
        crc32c
        json
)
set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)
