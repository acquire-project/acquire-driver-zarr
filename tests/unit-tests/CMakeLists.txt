set(project acquire-zarr)

set(tests
        thread-pool-push-to-job-queue
        validate-dimension
)

foreach (name ${tests})
    set(tgt "${project}-unit-test-${name}")
    add_executable(${tgt} ${name}.cpp)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    target_include_directories(${tgt} PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_SOURCE_DIR}/src/internal)
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr)

    add_test(NAME test-${tgt} COMMAND ${tgt})
    set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;unit-tests;acquire-zarr")
endforeach ()