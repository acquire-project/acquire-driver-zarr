set(tgt acquire-zarr)

add_library(${tgt}
        macros.hh
        acquire.zarr.cpp
        zarr.stream.hh
        zarr.stream.cpp
        zarr.common.hh
        zarr.common.cpp
        blosc.compression.params.hh
        blosc.compression.params.cpp
        thread.pool.hh
        thread.pool.cpp
        s3.connection.hh
        s3.connection.cpp
        sink.hh
        file.sink.hh
        file.sink.cpp
        s3.sink.hh
        s3.sink.cpp
        sink.creator.hh
        sink.creator.cpp
        array.writer.hh
        array.writer.cpp
        zarrv2.array.writer.hh
        zarrv2.array.writer.cpp
)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/logger>
)

target_link_libraries(${tgt} PRIVATE
        acquire-zarr-logger
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
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION include
        FILES_MATCHING PATTERN "*.h"
)