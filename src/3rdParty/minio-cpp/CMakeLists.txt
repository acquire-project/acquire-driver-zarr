set(pwd ${CMAKE_CURRENT_LIST_DIR})

set(lib "unknown-minio-cpp-lib")

if(WIN32)
    set(lib "${pwd}/lib/win64/miniocpp.dll")
    set(libd "${pwd}/lib/win64/miniocpp-debug.dll")
elseif(APPLE)
    set(lib "${pwd}/lib/osx/libminiocpp.a")
    set(libd "${pwd}/lib/osx/libminiocpp-debug.a")
elseif(LINUX)
    set(lib "${pwd}/lib/linux-amd64/libminiocpp.a")
    set(libd "${pwd}/lib/linux-amd64/libminiocpp-debug.a")
endif()

message(STATUS "minio-cpp: ${pwd}")
set(tgt miniocpp::miniocpp)
add_library(${tgt} SHARED IMPORTED GLOBAL)
target_include_directories(${tgt} INTERFACE ${pwd}/inc)
target_link_libraries(${tgt} INTERFACE ${MINIO_CPP_LIBS})
set_target_properties(${tgt} PROPERTIES
        IMPORTED_LOCATION ${lib}
        IMPORTED_LOCATION_DEBUG ${libd}
)