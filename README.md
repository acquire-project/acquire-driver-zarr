# Acquire Zarr Driver

This is an Acquire Driver that supports chunked streaming to [zarr][].

Compression is done via [blosc][].

## Devices

### Storage

- **Zarr**
- **ZarrBlosc1ZstdByteShuffle**
- **ZarrBlosc1Lz4ByteShuffle**

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html
[blosc]: https://github.com/Blosc/c-blosc
