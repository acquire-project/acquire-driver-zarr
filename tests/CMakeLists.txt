if (${NOTEST})
    message(STATUS "Skipping test targets")
else ()
    #    add_subdirectory(driver)

    add_subdirectory(unit-tests)
    add_subdirectory(integration)
endif ()
