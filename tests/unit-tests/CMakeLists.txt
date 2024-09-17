set(project acquire-zarr)

set(tests
        set-stream-parameters
        get-stream-parameters
        create-stream
        stream-get-settings
        common-chunk-lattice-index
        common-tile-group-offset
        common-chunk-internal-offset
        common-shard-index-for-chunk
        common-shard-internal-index
        thread-pool-push-to-job-queue
        s3-connection-put-object
        s3-connection-upload-multipart-object
        file-sink-write
        s3-sink-write
        sink-creator-make-metadata-sinks
        sink-creator-make-data-sinks
        array-writer-downsample-writer-config
        array-writer-write-frame-to-chunks
        zarrv2-writer-write-even
        zarrv2-writer-write-ragged-append-dim
        zarrv2-writer-write-ragged-internal-dim
)

foreach (name ${tests})
    set(tgt "${project}-unit-test-${name}")
    add_executable(${tgt} ${name}.cpp unit.test.macros.hh)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/src/logger
            ${CMAKE_SOURCE_DIR}/src/streaming
    )
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr-logger
            acquire-zarr
            miniocpp::miniocpp
    )

    add_test(NAME test-${tgt} COMMAND ${tgt})
    set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;unit-tests;acquire-zarr")
endforeach ()