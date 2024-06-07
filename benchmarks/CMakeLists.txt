option(WITH_BENCHMARKS "Build benchmarks" OFF)

if (${WITH_BENCHMARKS})
    #
    # PARAMETERS
    #
    set(project acquire-driver-zarr) # CMAKE_PROJECT_NAME gets overridden if this is a subtree of another project

    set(libtgt acquire-storage-benchmark)
    add_library(${libtgt} STATIC
            benchmark.storage.hh
            benchmark.storage.cpp
    )
    set_target_properties(${libtgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL"
    )
    add_dependencies(${libtgt} acquire-core-logger acquire-core-platform acquire-device-kit acquire-device-properties)
    target_include_directories(${libtgt} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../")
    target_link_libraries(${libtgt}
            acquire-core-logger
            acquire-core-platform
            acquire-device-kit
            acquire-device-hal
            acquire-device-properties
    )

    #
    # Benchmarks
    #
    set(benchmarks
            zarr-v2-filesystem
            zarr-v3-filesystem
            zarr-v2-s3
            zarr-v3-s3
    )

    include(CMakePrintHelpers)
    cmake_print_variables(CMAKE_CURRENT_LIST_DIR)

    foreach (name ${benchmarks})
        set(tgt "${project}-${name}")
        add_executable(${tgt} ${name}.cpp)
        target_compile_definitions(${tgt} PUBLIC "BENCHMARK=\"${tgt}\"")
        set_target_properties(${tgt} PROPERTIES
                MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL"
        )
        target_include_directories(${tgt} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../")
        target_link_libraries(${tgt}
                acquire-core-logger
                acquire-core-platform
                acquire-video-runtime
                ${libtgt}
                ${AWSSDK_LINK_LIBRARIES}
        )
    endforeach ()

    #
    # Copy driver to benchmarks directory
    #
    list(GET benchmarks 0 onename)

    foreach (driver
            acquire-driver-common
            acquire-driver-zarr
            ${AWSSDK_LINK_LIBRARIES}
    )
        add_custom_target(${project}-copy-${driver}-for-benchmarks
                COMMAND ${CMAKE_COMMAND} -E copy
                $<TARGET_FILE:${driver}>
                $<TARGET_FILE_DIR:${project}-${onename}>
                DEPENDS ${driver}
                COMMENT "Copying ${driver} to $<TARGET_FILE_DIR:${project}-${onename}>"
        )

        foreach (name ${benchmarks})
            add_dependencies(${tgt} ${project}-copy-${driver}-for-benchmarks)
        endforeach ()
    endforeach ()

    if (MSVC)
        AWSSDK_CPY_DYN_LIBS(SERVICE_COMPONENTS "" ${CMAKE_CURRENT_BINARY_DIR})
    endif ()
endif ()
