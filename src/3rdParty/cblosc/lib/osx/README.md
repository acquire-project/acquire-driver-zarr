The library here was built as follows.

Against v1.21.5 (d306135) of the c-blosc source hosted at:
https://github.com/Blosc/c-blosc

It's a universal binary compiled for `arm64` and `x86_64`.

If you're rebuilding this later, configure with:

```
CMAKE_OSX_ARCHITECTURES="arm64;x86_64"
```

The compiler used was:

```
Apple clang version 14.0.3 (clang-1403.0.22.14.1)
Target: arm64-apple-darwin22.5.0
Thread model: posix
```
