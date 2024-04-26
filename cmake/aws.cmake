list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../src/3rdParty/aws-sdk-cpp/lib/cmake/AWSSDK")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_LIST_DIR}/../src/3rdParty/aws-sdk-cpp")
message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
#message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")
set(AWSSDK_ROOT_DIR "${CMAKE_CURRENT_LIST_DIR}/../src/3rdParty/aws-sdk-cpp")

# Use the MSVC variable to determine if this is a Windows build.
set(WINDOWS_BUILD ${MSVC})

if (WINDOWS_BUILD) # Set the location where CMake can find the installed libraries for the AWS SDK.
    string(REPLACE ";" "/aws-cpp-sdk-all;" SYSTEM_MODULE_PATH "${CMAKE_SYSTEM_PREFIX_PATH}/aws-cpp-sdk-all")
    list(APPEND CMAKE_PREFIX_PATH ${SYSTEM_MODULE_PATH})
endif ()
set(SERVICE_COMPONENTS s3)

# Find the AWS SDK for C++ package.
find_package(AWSSDK REQUIRED COMPONENTS ${SERVICE_COMPONENTS})

if (WINDOWS_BUILD)
    # Copy relevant AWS SDK for C++ libraries into the current binary directory for running and debugging.

    set(BIN_SUB_DIR "/Debug") # if you are building from the command line you may need to uncomment this
    # and set the proper subdirectory to the executables' location.

    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}${BIN_SUB_DIR})
endif ()

message(STATUS "Found AWS SDK for C++ version ${AWSSDK_VERSION}")
message(STATUS "AWS link libraries ${AWSSDK_LINK_LIBRARIES}")

#add_executable(${PROJECT_NAME}
#        hello_s3.cpp)
#
#target_link_libraries(${PROJECT_NAME}
#        ${AWSSDK_LINK_LIBRARIES})