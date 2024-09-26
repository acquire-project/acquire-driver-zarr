add_subdirectory(logger)
add_subdirectory(streaming)

if (BUILD_ACQUIRE_DRIVER_ZARR)
    add_subdirectory(driver)
endif ()
