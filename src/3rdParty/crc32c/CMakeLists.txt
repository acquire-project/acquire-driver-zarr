set(pwd ${CMAKE_CURRENT_LIST_DIR})

set(lib "unknown-crc32c-lib")

if(WIN32)
    set(lib "${pwd}/lib/win64/crc32c.lib")
    set(libd "${pwd}/lib/win64/crc32c-debug.lib")
elseif(APPLE)
    set(lib "${pwd}/lib/osx/libcrc32c.a")
    set(libd ${lib})
elseif(LINUX)
    set(lib "${pwd}/lib/linux-amd64/libcrc32c.a")
    set(libd ${lib})
endif()

message(STATUS "crc32c: ${pwd}")
set(tgt crc32c)
add_library(${tgt} STATIC IMPORTED GLOBAL)
target_include_directories(${tgt} INTERFACE ${pwd}/inc)
set_target_properties(${tgt} PROPERTIES
    IMPORTED_LOCATION ${lib}
    IMPORTED_LOCATION_DEBUG ${libd}
)
