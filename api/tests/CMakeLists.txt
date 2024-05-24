if (${NOTEST})
    message(STATUS "Skipping test targets")
else ()
    project(acquire-zarr-api LANGUAGES C)
    #
    # Tests
    #
    set(tests
            test-api
    )


    foreach (name ${tests})
        set(tgt "${PROJECT_NAME}-${name}")
        add_executable(${tgt} ${name}.c)
        target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
        set_target_properties(${tgt} PROPERTIES
                MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        )

        target_include_directories(${tgt} PUBLIC "${CMAKE_CURRENT_LIST_DIR}/../include")

        target_link_libraries(${tgt}
                acquire-zarr
        )

        add_test(NAME test-${tgt} COMMAND ${tgt})
        set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;acquire-zarr-api")
    endforeach ()

 endif ()
