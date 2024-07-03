set(pwd ${CMAKE_CURRENT_LIST_DIR})

set(lib "unknown-minio-cpp-lib")

if(WIN32)
    set(lib "${pwd}/lib/win64/miniocpp.lib")
    set(libd "${pwd}/lib/win64/miniocpp-debug.lib")
elseif(APPLE)
    set(lib "${pwd}/lib/osx/miniocpp.a")
    set(libd "${pwd}/lib/osx/miniocpp-debug.a")
elseif(LINUX)
    set(lib "${pwd}/lib/linux-amd64/miniocpp.a")
    set(libd "${pwd}/lib/linux-amd64/miniocpp-debug.a")
endif()

# Dependencies
# ------------

find_package(OpenSSL REQUIRED)
find_package(unofficial-curlpp CONFIG REQUIRED)
find_package(unofficial-inih CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(pugixml CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

list(APPEND MINIO_CPP_LIBS
        unofficial::curlpp::curlpp
        unofficial::inih::inireader
        nlohmann_json::nlohmann_json
        pugixml
        OpenSSL::SSL OpenSSL::Crypto
        ZLIB::ZLIB
)

if (WIN32)
    list(APPEND MINIO_CPP_LIBS wsock32)
    list(APPEND MINIO_CPP_LIBS ws2_32)
endif()

# ------------

message(STATUS "minio-cpp: ${pwd}")
set(tgt miniocpp::miniocpp)
add_library(${tgt} STATIC IMPORTED GLOBAL)
target_include_directories(${tgt} INTERFACE ${pwd}/inc)
target_link_libraries(${tgt} INTERFACE ${MINIO_CPP_LIBS})
set_target_properties(${tgt} PROPERTIES
        IMPORTED_LOCATION ${lib}
        IMPORTED_LOCATION_DEBUG ${libd}
)