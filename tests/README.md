# Testing the Zarr driver

To run the tests, you can run:

```bash
ctest -L acquire-driver-zarr --output-on-failure
```

You can disable unit tests by setting `-DNO_UNIT_TESTS=ON` when configuring the project.

## Running the S3 tests

To run the S3 tests, you will need to define the following constants in `tests/credentials.hpp`:

```cpp
#define ZARR_S3_ENDPOINT "my endpoint"
#define ZARR_S3_ACCESS_KEY_ID "my access key id"
#define ZARR_S3_SECRET_ACCESS_KEY "my secret access key"
```

You will need to be able to create and delete buckets in the S3 endpoint you are using.

If you do not intend to run the S3 tests, you can simply not create the `tests/credentials.hpp` file.

There are some unit tests that also require an S3 endpoint and credentials, but they can't see `tests/credentials.hpp`.
If you want to run them, you will have to copy `tests/credentials.hpp` to `src/credentials.hpp`.
