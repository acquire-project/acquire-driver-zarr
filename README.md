# Acquire Zarr Driver

[![Build](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml)
[![Tests](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml)

This is an Acquire Driver that supports chunked streaming to [zarr][].

Compression is done via [blosc][].

## Devices

### Storage

- **Zarr**
- **ZarrBlosc1ZstdByteShuffle**
- **ZarrBlosc1Lz4ByteShuffle**

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html
[blosc]: https://github.com/Blosc/c-blosc
