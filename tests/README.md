# Testing the Zarr driver

To run the tests, you can run:

```bash
ctest -L acquire-driver-zarr --output-on-failure
```

You can disable unit tests by setting `-DNO_UNIT_TESTS=ON` when configuring the project.
You can disable testing altogether by setting `-DNOTEST=ON`.
