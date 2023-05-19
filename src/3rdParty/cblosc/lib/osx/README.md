The library here was built as follows.

Against v1.21.4 (2c2f9bd) of the c-blosc source hosted at:
https://github.com/Blosc/c-blosc

It's a universal binary compiled for `arm64` and `x86_64`.

If you're rebuilding this later, configure with:

```
CMAKE_OSX_ARCHITECTURE="arm64;x86_64"
```

The compiler used was:

```
Apple clang version 14.0.0 (clang-1400.0.29.202)
Target: x86_64-apple-darwin21.6.0
Thread model: posix
```
