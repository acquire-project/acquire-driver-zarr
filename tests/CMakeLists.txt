if (${NOTEST})
    message(STATUS "Skipping test targets")
else ()
#    add_subdirectory(driver)

    set(project acquire-zarr)

    set(tests
        get_and_set_parameters
    )

    foreach (name ${tests})
        set(tgt "${project}-${name}")
        add_executable(${tgt} ${name}.cpp)
        target_include_directories(${tgt} PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../src")
        target_link_libraries(${tgt} acquire-zarr)

        add_test(NAME test-${tgt} COMMAND ${tgt})
        set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;acquire-zarr")
    endforeach ()
endif ()
