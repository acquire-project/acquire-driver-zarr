set(tgt acquire-zarr)

add_library(${tgt}
        logger.hh
        logger.cpp
        stream.settings.hh
        stream.settings.cpp
        zarr.stream.hh
        zarr.stream.cpp
)

set(PUBLIC_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include/)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${PUBLIC_INCLUDE_DIR}>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_link_libraries(${tgt} PRIVATE
        blosc_static
        miniocpp::miniocpp
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt}
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

# Install public header files
install(DIRECTORY ${PUBLIC_INCLUDE_DIR}
        DESTINATION include
        FILES_MATCHING PATTERN "*.h"
)