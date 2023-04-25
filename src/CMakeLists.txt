add_subdirectory(3rdParty)

if(NOT TARGET acquire-core-logger)
        add_subdirectory(acquire-core-libs)
endif()

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        prelude.h
        zarr.hh
        zarr.cpp
        zarr.raw.hh
        zarr.raw.cpp
        zarr.codec.hh
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
