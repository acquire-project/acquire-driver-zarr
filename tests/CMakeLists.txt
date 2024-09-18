if (${NOTEST})
    message(STATUS "Skipping test targets")
else ()
    add_subdirectory(unit-tests)

    if (BUILD_ACQUIRE_DRIVER_ZARR)
        add_subdirectory(driver)
    endif ()
endif ()
