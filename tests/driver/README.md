# Testing the Zarr driver

To run the tests, you can run:

```bash
ctest -L acquire-driver-zarr --output-on-failure
```

You can disable unit tests by setting `-DNO_UNIT_TESTS=ON` when configuring the project.
You can disable testing altogether by setting `-DNOTEST=ON`.

## Testing S3 components

To test the S3 components, you need to set the following environment variables
to point to your S3 bucket:

- ZARR_S3_ENDPOINT
- ZARR_S3_BUCKET_NAME
- ZARR_S3_ACCESS_KEY_ID
- ZARR_S3_SECRET_ACCESS_KEY

with the appropriate values.
