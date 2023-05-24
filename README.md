# Acquire Zarr Driver

[![Build](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml)
[![Tests](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml)

This is an Acquire Driver that supports chunked streaming to [zarr][].

## Devices

### Storage

- **Zarr**
- **ZarrBlosc1ZstdByteShuffle**
- **ZarrBlosc1Lz4ByteShuffle**

## Using the Zarr storage device

Zarr has additional capabilities relative to the basic storage devices, namely _chunking_ and _compression_.
These can be configured using the `storage_properties_set_chunking_props()`
and `storage_properties_set_compression_props()` functions, respectively.

### Configuring chunking

You can configure chunking by calling `storage_properties_set_chunking_props()` on your Zarr `Storage` object _after_
calling `storage_properties_init()`.
There are 4 parameters you can set to determine the chunk size, namely `tile_width`, `tile_height`, `tile_planes`,
and `bytes_per_chunk`:

```c
int
storage_properties_set_chunking_props(struct StorageProperties* out,
                                      uint32_t tile_width,
                                      uint32_t tile_height,
                                      uint32_t tile_planes,
                                      uint32_t bytes_per_chunk)
```

A _tile_ is a contiguous section, or region of interest, of a _frame_.
A _chunk_ is nothing more than some number of stacked tiles from subsequent frames, with each tile in a chunk having
the same ROI in its respective frame.

You can specify the width and height, in pixels, of each tile, and if your frame size has more than one plane, you can
specify the number of planes you want per tile as well.
If any of these values are unset, or if they are set to a value larger than the frame size, the full value of the frame
size along that dimension will be used instead.
You should take care that the values you select won't result in tile sizes that are too small or too large for your
application.

The `bytes_per_chunk` parameter can be used to cap the size of a chunk.
A minimum of 16 MiB is enforced, but no maximum, so if you are compressing you must ensure that you have sufficient
memory for all your chunks to be stored in memory at once.

### Compression

Compression is done via [Blosc][].
Supported codecs are **lz4** and **zstd**, which can be used by using the **ZarrBlosc1Lz4ByteShuffle** and
**ZarrBlosc1ZstdByteShuffle** devices, respectively.
For a comparison of these codecs, please refer to the [Blosc docs][Blosc].

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html

[Blosc]: https://github.com/Blosc/c-blosc
