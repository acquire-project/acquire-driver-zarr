add_subdirectory(3rdParty)

if (NOT TARGET acquire-core-logger)
    add_subdirectory(../acquire-common/acquire-core-libs ${CMAKE_CURRENT_BINARY_DIR}/acquire-core-libs)
endif ()

ExternalProject_Get_Property(aws-sdk-cpp INSTALL_DIR)
SET(AWS_OUTPUT_DIR ${INSTALL_DIR}/../aws-sdk-cpp-install) 
INCLUDE_DIRECTORIES(${AWS_OUTPUT_DIR}/include)
LINK_DIRECTORIES(${AWS_OUTPUT_DIR}/lib)


set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        common.hh
        common.cpp
        writers/sink.hh
        writers/file.sink.hh
        writers/file.sink.cpp
        writers/s3.sink.hh
        writers/writer.hh
        writers/writer.cpp
        writers/zarrv2.writer.hh
        writers/zarrv2.writer.cpp
        writers/zarrv3.writer.hh
        writers/zarrv3.writer.cpp
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
add_dependencies(${tgt} acquire-core-logger acquire-core-platform acquire-device-kit acquire-device-properties cblosc json aws-sdk-cpp)
target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        cblosc
        json
#        ${AWSSDK_LINK_LIBRARIES}
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)
