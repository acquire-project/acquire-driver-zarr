# MinIO C++ client

This repository includes headers and pre-build binaries for the MinIO C++ client SDK.
It was built from
commit [cd4ef14955201d5ce81e54155ef0980f7c644864](https://github.com/minio/minio-cpp/tree/cd4ef14955201d5ce81e54155ef0980f7c644864).
In the base CMakeLists.txt, it was necessary to change `STATIC` to `MODULE` in the `add_library` command to build the
shared library.

## Building

### Windows

To generate the solution file, run the following command:

```shell
cmake . -B build -DMINIO_CPP_TEST=OFF -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" -DVCPKG_INSTALLED_DIR=vcpkg_installed -DVCPKG_TARGET_TRIPLET=x64-windows-static
```

Then, open the solution file in Visual Studio.
Open the Solution Explorer, select `miniocpp` and right-click on it.
Select `Configuration Properties > C/C++ > Code Generation`.
For Release type builds, include RelWithDebInfo, change the `Runtime Library` to `Multi-threaded (/MT)` in order to
statically link the MSVC runtime.
For Debug builds, change the `Runtime Library` to `Multi-threaded Debug (/MTd)`.
Finally, build the solution.

### macOS

To generate the universal binary, you will need to build the library for both x86_64 and arm64 architectures.
Run the following commands:

```shell
# Build for x86_64
cmake . -B build-x64 -DMINIO_CPP_TEST=OFF -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" -DVCPKG_INSTALLED_DIR=vcpkg-x64 -DVCPKG_TARGET_TRIPLET=x64-osx -DCMAKE_OSX_ARCHITECTURES="x86_64"  -DCMAKE_BUILD_TYPE=Release # or Debug
cmake --build build-x64 --config Release # or Debug
# Repeat the process for arm64
cmake . -B build-arm64 -DMINIO_CPP_TEST=OFF -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" -DVCPKG_INSTALLED_DIR=vcpkg-arm64 -DVCPKG_TARGET_TRIPLET=arm64-osx -DCMAKE_OSX_ARCHITECTURES="arm64"  -DCMAKE_BUILD_TYPE=Release # or Debug
cmake --build build-arm64 --config Release # or Debug
# Create the universal binary
lipo -create build-x64/libminiocpp.a build-arm64/libminiocpp.a -output libminiocpp.a
```

### Linux

To generate the shared library, run the following command:

```shell
cmake . -B build -DMINIO_CPP_TEST=OFF -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" -DCMAKE_BUILD_TYPE=Release # or Debug
cmake --build build --config Release # or Debug
```