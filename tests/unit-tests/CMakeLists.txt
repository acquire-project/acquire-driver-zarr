set(project acquire-zarr)

set(tests
        validate-dimension
        thread-pool-push-to-job-queue
        file-sink-write
        s3-connection-put-object
        s3-connection-upload-multipart-object
        s3-sink-write
        sink-creator-make-metadata-sinks
        sink-creator-make-data-sinks
)

foreach (name ${tests})
    set(tgt "${project}-unit-test-${name}")
    add_executable(${tgt} ${name}.cpp)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_SOURCE_DIR}/src/internal)
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr
            miniocpp::miniocpp
    )

    add_test(NAME test-${tgt} COMMAND ${tgt})
    set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;unit-tests;acquire-zarr")
endforeach ()