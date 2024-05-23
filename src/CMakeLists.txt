set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_subdirectory(3rdParty)

if (NOT TARGET acquire-core-logger)
    add_subdirectory(../acquire-common/acquire-core-libs ${CMAKE_CURRENT_BINARY_DIR}/acquire-core-libs)
endif ()


set(tgt acquire-driver-zarr)

#COMPILE SOURCES AS AN OBJECT LIBRARY
add_library(${tgt}-obj OBJECT 
        common.hh
        common.cpp
        writers/sink.hh
        writers/file.sink.hh
        writers/file.sink.cpp
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

# establish a list of dependencies (which importantly are CMake projects)
set(${tgt}_deps
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        cblosc
        json
)

# TOD0: this will get the includes for all dependencies listed above.
# foreach(dep ${${tgt}_deps})
#     set(tmp ${${dep}_SOURCE_DIR})
#     list(APPEND ${tgt}_inc ${tmp})
# endforeach()

# ugly:  get the platform
if(WIN32)
    set(my_platform win32)
elseif(APPLE)
        set(my_platform osx)
elseif(LINUX)
        set(my_platform linux)
endif()

target_include_directories(${tgt}-obj PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${CMAKE_CURRENT_LIST_DIR}/writers
        ${CMAKE_CURRENT_LIST_DIR}/../acquire-common/acquire-core-libs/src/acquire-core-logger
        ${CMAKE_CURRENT_LIST_DIR}/../acquire-common/acquire-core-libs/src/acquire-core-platform/${my_platform}
        ${CMAKE_CURRENT_LIST_DIR}/../acquire-common/acquire-core-libs/src/acquire-device-kit
        ${CMAKE_CURRENT_LIST_DIR}/../acquire-common/acquire-core-libs/src/acquire-device-properties
        ${CMAKE_CURRENT_LIST_DIR}/../src/3rdParty/cblosc/inc
        ${CMAKE_CURRENT_LIST_DIR}/../src/3rdParty/json/inc
)

add_library(${tgt} MODULE $<TARGET_OBJECTS:${tgt}-obj>)

target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        ${${tgt}_deps}
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)
