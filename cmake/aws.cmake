#set(SERVICE_COMPONENTS s3)
#set(AWS_SDK_DIR "${CMAKE_CURRENT_LIST_DIR}/../aws-sdk-cpp")
#file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build)
#
#set(ARGS "-DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install -DBUILD_ONLY=${SERVICE_COMPONENTS} -DENABLE_TESTING=OFF")
#
#execute_process(COMMAND "${CMAKE_COMMAND}" ${ARGS} ${AWS_SDK_DIR}
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build
#)
#execute_process(COMMAND ${CMAKE_COMMAND} --build ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build --config ${CMAKE_BUILD_TYPE} --target install
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build
#)
#


#if (NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/lib/cmake/AWSSDK/AWSSDKConfig.cmake")
#    if (WIN32)
#        if (CMAKE_BUILD_TYPE STREQUAL "Debug")
#            set(AWS_SDK_URI "https://github.com/acquire-project/acquire-driver-zarr/actions/runs/9085893115/artifacts/1502716272")
#        elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
#            set(AWS_SDK_URI "https://github.com/acquire-project/acquire-driver-zarr/actions/runs/9085893115/artifacts/1502716281")
#        else () # Release
#            set(AWS_SDK_URI "https://github.com/acquire-project/acquire-driver-zarr/actions/runs/9085893115/artifacts/1502716277")
#        endif ()
#    elseif (LINUX)
#        message(STATUS "wip: Linux")
#    else () # MacOS
#        message(STATUS "wip: MacOS")
#    endif ()
#
#    # Download and extract AWS SDK for C++ libraries
#    set(AWS_SDK_DIR "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp")
#    file(MAKE_DIRECTORY ${AWS_SDK_DIR})
#    file(DOWNLOAD ${AWS_SDK_URI} ${AWS_SDK_DIR}/aws-sdk-cpp.zip)
#endif ()
#
#find_package(AWSSDK REQUIRED COMPONENTS s3)
#if (MSVC)
#    # Copy relevant AWS SDK for C++ libraries into the current binary directory for running and debugging.
#
#    #    set(BIN_SUB_DIR "/Debug") # if you are building from the command line you may need to uncomment this
#    # and set the proper subdirectory to the executables' location.
#
#    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}${BIN_SUB_DIR})
#    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}/tests)
#endif ()

include(CMakePrintHelpers)
#if (WIN32)
#    execute_process(COMMAND "where.exe" "pwsh" OUTPUT_VARIABLE PWSH OUTPUT_STRIP_TRAILING_WHITESPACE)
#    cmake_print_variables(PWSH)
#endif ()

cmake_print_variables(CMAKE_CURRENT_LIST_DIR)

if (NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install/lib/cmake/AWSSDK/AWSSDKConfig.cmake")
    if (WIN32)
        #        EXECUTE_PROCESS(COMMAND "where.exe" "pwsh" OUTPUT_VARIABLE PWSH OUTPUT_STRIP_TRAILING_WHITESPACE)
        EXECUTE_PROCESS(COMMAND "pwsh.exe" "${CMAKE_CURRENT_LIST_DIR}/../scripts/build-aws.ps1" "-BuildType" "${CMAKE_BUILD_TYPE}" "-InstallDir" "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")
    else ()
        EXECUTE_PROCESS(COMMAND "bash" "${CMAKE_CURRENT_LIST_DIR}/../scripts/build-aws.sh" "${CMAKE_BUILD_TYPE}" "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")
    endif ()
endif ()

list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")

find_package(AWSSDK REQUIRED COMPONENTS s3)