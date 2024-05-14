set(SERVICE_COMPONENTS s3)
set(AWS_SDK_DIR "${CMAKE_CURRENT_LIST_DIR}/../aws-sdk-cpp")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build)

set(ARGS "-DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install -DBUILD_ONLY=${SERVICE_COMPONENTS} -DENABLE_TESTING=OFF")

execute_process(COMMAND "${CMAKE_COMMAND}" ${ARGS} ${AWS_SDK_DIR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build
)
execute_process(COMMAND ${CMAKE_COMMAND} --build ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build --config ${CMAKE_BUILD_TYPE} --target install
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-build
)

list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp-install")
find_package(AWSSDK REQUIRED COMPONENTS s3)
if (MSVC)
    # Copy relevant AWS SDK for C++ libraries into the current binary directory for running and debugging.

    #    set(BIN_SUB_DIR "/Debug") # if you are building from the command line you may need to uncomment this
    # and set the proper subdirectory to the executables' location.

    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}${BIN_SUB_DIR})
    AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR}/tests)
endif ()