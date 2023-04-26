add_subdirectory(3rdParty)

if(NOT TARGET acquire-core-logger)
        add_subdirectory(acquire-core-libs)
endif()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        prelude.h
        tiled.frame.hh
        tiled.frame.cpp
        zarr.hh
        zarr.cpp
        zarr.encoder.hh
        zarr.encoder.cpp
        chunk.writer.hh
        chunk.writer.cpp
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
