set(AWS_SDK_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install)

if (NOT EXISTS "${AWS_SDK_INSTALL_DIR}/lib/cmake/AWSSDK/AWSSDKConfig.cmake")
    if (WIN32)
        EXECUTE_PROCESS(COMMAND "pwsh.exe" "${CMAKE_CURRENT_LIST_DIR}/../scripts/build-aws.ps1" "-BuildType" "${CMAKE_BUILD_TYPE}" "-InstallDir" "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")
    else ()
        EXECUTE_PROCESS(COMMAND "bash" "${CMAKE_CURRENT_LIST_DIR}/../scripts/build-aws.sh" "${CMAKE_BUILD_TYPE}" "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")
    endif ()
endif ()

list(APPEND CMAKE_PREFIX_PATH "${AWS_SDK_INSTALL_DIR}")

find_package(AWSSDK REQUIRED COMPONENTS s3)

if (WIN32)
    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}/src)
    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}/tests)
endif ()