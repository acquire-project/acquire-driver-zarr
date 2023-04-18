set(pwd ${CMAKE_CURRENT_LIST_DIR})

set(lib "unknown-cblosc-lib")

if(WIN32)
    set(lib "${pwd}/lib/win64/libblosc.lib")
    set(libd "${pwd}/lib/win64/libblosc-debug.lib")
elseif(APPLE)
    set(lib "${pwd}/lib/osx/libblosc.a")
    set(libd ${lib})
elseif(LINUX)
    set(lib "${pwd}/lib/linux-amd64/libblosc.a")
    set(libd ${lib})
endif()

message(STATUS "C-Blosc: ${pwd}")
set(tgt cblosc)
add_library(${tgt} STATIC IMPORTED GLOBAL)
target_include_directories(${tgt} INTERFACE ${pwd}/inc)
set_target_properties(${tgt} PROPERTIES
    IMPORTED_LOCATION ${lib}
    IMPORTED_LOCATION_DEBUG ${libd}
)
