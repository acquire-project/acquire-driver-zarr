set(tgt acquire-zarr)
add_library(${tgt} STATIC
        include/zarr.h
        include/zarr_errors.h
        internal/logger.hh
        internal/logger.cpp
        internal/zarr.common.c
        internal/stream.settings.hh
        internal/stream.settings.cpp
        internal/zarr.stream.hh
        internal/zarr.stream.cpp
        internal/thread.pool.hh
        internal/thread.pool.cpp
        internal/s3.connection.hh
        internal/s3.connection.cpp
        internal/blosc.compressor.hh
        internal/blosc.compressor.cpp
        internal/sink.hh
        internal/file.sink.hh
        internal/file.sink.cpp
        internal/s3.sink.hh
        internal/s3.sink.cpp
        internal/sink.creator.hh
        internal/sink.creator.cpp
#        internal/array.writer.hh
#        internal/array.writer.cpp
)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/internal>
)

target_link_libraries(${tgt} PRIVATE
        blosc_static
        miniocpp::miniocpp
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)

#if (NOT TARGET acquire-core-logger)
#    add_subdirectory(../acquire-common/acquire-core-libs ${CMAKE_CURRENT_BINARY_DIR}/acquire-core-libs)
#endif ()

#set(tgt-driver acquire-driver-zarr)
#add_library(${tgt-driver} MODULE
#        common/dimension.hh
#        common/dimension.cpp
#        common/s3.connection.hh
#        common/s3.connection.cpp
#        common/utilities.hh
#        common/utilities.cpp
#        writers/sink.hh
#        writers/sink.creator.hh
#        writers/sink.creator.cpp
#        writers/file.sink.hh
#        writers/file.sink.cpp
#        writers/s3.sink.hh
#        writers/s3.sink.cpp
#        writers/array.writer.hh
#        writers/array.writer.cpp
#        writers/zarrv2.array.writer.hh
#        writers/zarrv2.array.writer.cpp
#        writers/zarrv3.array.writer.hh
#        writers/zarrv3.array.writer.cpp
#        writers/blosc.compressor.hh
#        writers/blosc.compressor.cpp
#        driver/zarr.hh
#        driver/zarr.cpp
#        driver/zarr.v2.hh
#        driver/zarr.v2.cpp
#        driver/zarr.v3.hh
#        driver/zarr.v3.cpp
#        driver/zarr.driver.c
#)
#
#target_include_directories(${tgt-driver} PRIVATE
#        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
#)
#
#target_enable_simd(${tgt-driver})
#target_link_libraries(${tgt-driver} PRIVATE
#        acquire-core-logger
#        acquire-core-platform
#        acquire-device-kit
#        acquire-device-properties
#        blosc_static
#        nlohmann_json::nlohmann_json
#        miniocpp::miniocpp
#)
set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)
#
#install(TARGETS ${tgt-driver} LIBRARY DESTINATION lib)
