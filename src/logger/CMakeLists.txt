set(tgt acquire-zarr-logger)

add_library(${tgt}
        logger.hh
        logger.cpp
)

set(PUBLIC_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include/)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${PUBLIC_INCLUDE_DIR}>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt}
        LIBRARY DESTINATION lib
)