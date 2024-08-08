if (${NOTEST})
    message(STATUS "Skipping test targets")
else ()
    set(NOTEST "TRUE")
    add_subdirectory(../acquire-common/acquire-driver-common ${CMAKE_CURRENT_BINARY_DIR}/acquire-driver-common)
    add_subdirectory(../acquire-common/acquire-video-runtime ${CMAKE_CURRENT_BINARY_DIR}/acquire-video-runtime)
    set(NOTEST "FALSE")

    #
    # PARAMETERS
    #
    set(project acquire-driver-zarr) # CMAKE_PROJECT_NAME gets overridden if this is a subtree of another project

    #
    # Tests
    #
    set(tests
            list-devices
            unit-tests
            get
            get-meta
            external-metadata-with-whitespace-ok
            restart-stopped-zarr-resets-threadpool
            repeat-start
            metadata-dimension-sizes
            write-zarr-v2-raw
            write-zarr-v2-raw-chunk-size-larger-than-frame-size
            write-zarr-v2-raw-with-even-chunking
            write-zarr-v2-raw-with-even-chunking-and-rollover
            write-zarr-v2-raw-with-ragged-chunking
            write-zarr-v2-with-lz4-compression
            write-zarr-v2-with-zstd-compression
            write-zarr-v2-compressed-with-chunking
            write-zarr-v2-compressed-with-chunking-and-rollover
            write-zarr-v2-raw-multiscale
            write-zarr-v2-raw-multiscale-with-trivial-tile-size
            write-zarr-v2-compressed-multiscale
            write-zarr-v2-to-s3
            multiscales-metadata
            write-zarr-v3-raw
            write-zarr-v3-raw-with-ragged-sharding
            write-zarr-v3-raw-chunk-exceeds-array
            write-zarr-v3-compressed
            write-zarr-v3-raw-multiscale
            write-zarr-v3-to-s3
    )

    foreach (name ${tests})
        set(tgt "${project}-${name}")
        add_executable(${tgt} ${name}.cpp)
        target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
        set_target_properties(${tgt} PROPERTIES
                MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        )
        target_include_directories(${tgt} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../")
        target_link_libraries(${tgt}
                acquire-core-logger
                acquire-core-platform
                acquire-video-runtime
                nlohmann_json::nlohmann_json
                miniocpp::miniocpp
        )

        add_test(NAME test-${tgt} COMMAND ${tgt})
        set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;acquire-driver-zarr")
    endforeach ()

    #
    # Copy driver to tests
    #
    list(POP_FRONT tests onename)

    foreach (driver
            acquire-driver-common
            acquire-driver-zarr
    )
        add_custom_target(${project}-copy-${driver}-for-tests
                COMMAND ${CMAKE_COMMAND} -E copy
                $<TARGET_FILE:${driver}>
                $<TARGET_FILE_DIR:${project}-${onename}>
                DEPENDS ${driver}
                COMMENT "Copying ${driver} to $<TARGET_FILE_DIR:${project}-${onename}>"
        )

        foreach (name ${tests})
            add_dependencies(${tgt} ${project}-copy-${driver}-for-tests)
        endforeach ()
    endforeach ()
endif ()
