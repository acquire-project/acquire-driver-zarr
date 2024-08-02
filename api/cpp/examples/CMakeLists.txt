project(acquire-zarr-cpp-api-examples)

#set(tgt acquire-zarr-cpp)
set(ACQUIRE_API_INCLUDE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/include/acquire-zarr/acquire-zarr.hh)

set(examples
        simple
)

foreach (name ${examples})
        set(tgt "${name}")
        add_executable(${name} ${name}.cpp)

        set_target_properties(${tgt} PROPERTIES
                MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        )
        target_include_directories(${tgt} PRIVATE "${acquire-zarr-cpp-api_SOURCE_DIR}/include")
        target_link_libraries(${tgt}
                acquire-zarr-cpp
        )

        #add_test(NAME test-${tgt} COMMAND ${tgt})
        #set_tests_properties(test-${tgt} PROPERTIES LABELS "anyplatform;acquire-driver-zarr")
endforeach ()
