add_subdirectory(3rdParty)

if(NOT TARGET acquire-core-logger)
        add_subdirectory(acquire-core-libs)
endif()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        prelude.h
        v2/zarr.v2.hh
        v2/zarr.v2.cpp
        v3/zarr.v3.hh
        v3/zarr.v3.cpp
        tiled.frame.hh
        tiled.frame.cpp
        chunk.writer.hh
        chunk.writer.cpp
        frame.scaler.hh
        frame.scaler.cpp
        zarr.hh
        zarr.cpp
        zarr.encoder.hh
        zarr.encoder.cpp
        zarr.raw.hh
        zarr.raw.cpp
        zarr.blosc.hh
        zarr.blosc.cpp
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
