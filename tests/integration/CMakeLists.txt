set(project acquire-zarr)

set(tests
        set-and-get-params
        create-and-destroy-stream
)

foreach (name ${tests})
    set(tgt "${project}-${name}")
    add_executable(${tgt} ${name}.cpp)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            "${CMAKE_SOURCE_DIR}/src/include")
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr)

    add_test(NAME test-${tgt} COMMAND ${tgt})
    set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;acquire-zarr")
endforeach ()