enable_testing()

add_test(
    NAME python_api_test
    COMMAND pytest ${CMAKE_CURRENT_LIST_DIR}/test_zarr.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

set_tests_properties(python_api_test 
    PROPERTIES 
        LABELS "acquire-zarr-python-api" 
        ENVIRONMENT "PYTHONPATH=${acquire-zarr-py_BINARY_DIR};"
        DEPENDS acquire_zarr)

