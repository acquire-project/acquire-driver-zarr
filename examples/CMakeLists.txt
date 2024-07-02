if (${WITH_EXAMPLES})
    #
    # PARAMETERS
    #
    set(project acquire-driver-zarr) # CMAKE_PROJECT_NAME gets overridden if this is a subtree of another project

    #
    # Examples
    #
    set(examples
            no-striping
            bucket-exists
    )

    foreach (name ${examples})
        set(tgt "${project}-${name}")
        add_executable(${tgt} ${name}.cpp)
        target_compile_definitions(${tgt} PUBLIC "EXAMPLE=\"${tgt}\"")
        set_target_properties(${tgt} PROPERTIES
                MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        )
        target_include_directories(${tgt} PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src)
        target_link_libraries(${tgt}
                acquire-core-logger
                acquire-core-platform
                acquire-video-runtime
                miniocpp::miniocpp
                nlohmann_json::nlohmann_json
        )
    endforeach ()

    #
    # Copy driver to examples directory
    #
    list(GET examples 0 onename)

    foreach (driver
            acquire-driver-common
            acquire-driver-zarr
    )
        add_custom_target(${project}-copy-${driver}-for-examples
                COMMAND ${CMAKE_COMMAND} -E copy
                $<TARGET_FILE:${driver}>
                $<TARGET_FILE_DIR:${project}-${onename}>
                DEPENDS ${driver}
                COMMENT "Copying ${driver} to $<TARGET_FILE_DIR:${project}-${onename}>"
        )

        foreach (name ${examples})
            add_dependencies(${tgt} ${project}-copy-${driver}-for-examples)
        endforeach ()
    endforeach ()
endif ()
