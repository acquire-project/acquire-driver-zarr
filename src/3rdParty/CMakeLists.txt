add_subdirectory(cblosc)
add_subdirectory(json)


# see: https://sdk.amazonaws.com/cpp/api/LATEST/root/html/md_docs_2_c_make___external___project.html
ExternalProject_Add(
    aws-sdk-cpp
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/aws-sdk-cpp
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp
    LIST_SEPARATOR    "|"  # this is needed to separate targets specified in -DBUILD_ONLY below
    CMAKE_ARGS 
        -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install
        -DBUILD_ONLY=core|s3
        -DENABLE_TESTING=OFF
    )