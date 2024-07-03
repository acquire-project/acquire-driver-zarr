cmake_minimum_required(VERSION 3.23)
project(acquire-driver-zarr)
cmake_policy(SET CMP0079 NEW) # allows use with targets in other directories
enable_testing()

find_package(nlohmann_json CONFIG REQUIRED)
find_package(blosc CONFIG REQUIRED)
#find_package(miniocpp CONFIG REQUIRED) # TODO (aliddell): revert when minio-cpp is fixed

include(cmake/aq_require.cmake)
include(cmake/git-versioning.cmake)
include(cmake/ide.cmake)
include(cmake/install-prefix.cmake)
include(cmake/wsl.cmake)
include(cmake/simd.cmake)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(examples)

include(CPack)
