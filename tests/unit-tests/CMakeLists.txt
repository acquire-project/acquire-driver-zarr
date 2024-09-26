set(project acquire-zarr)

set(tests
        create-stream
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
            acquire-logger
            acquire-zarr
            miniocpp::miniocpp
    )

    add_test(NAME test-${tgt} COMMAND ${tgt})
    set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;unit-tests;acquire-zarr")
endforeach ()