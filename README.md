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
These can be configured using the `storage_properties_set_chunking_props()`, or by selecting one of the `ZarrBlosc1*`
devices, respectively.

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
                                      uint64_t max_bytes_per_chunk)
```

| ![frames](https://github.com/aliddell/acquire-driver-zarr/assets/844464/3510d468-4751-4fa0-b2bf-0e29a5f3ea1c) |
|:--:|
| A collection of frames. |

A _tile_ is a contiguous section, or region of interest, of a _frame_.

| ![tiles](https://github.com/aliddell/acquire-driver-zarr/assets/844464/f8d16139-e0ac-44db-855f-2f5ef305c98b) |
|:--:|
| A collection of frames, divided into tiles. |

A _chunk_ is nothing more than some number of stacked tiles from subsequent frames, with each tile in a chunk having
the same ROI in its respective frame.

|  ![chunks](https://github.com/aliddell/acquire-driver-zarr/assets/844464/653e4d82-363e-4e04-9a42-927b052fb6e7) |
|:--:|
| A collection of frames, divided into tiles. A single chunk has been highlighted in red. |

You can specify the width and height, in pixels, of each tile, and if your frame size has more than one plane, you can
specify the number of planes you want per tile as well.
If any of these values are unset (equivalently, set to 0), or if they are set to a value larger than the frame size,
the full value of the frame size along that dimension will be used instead.
You should take care that the values you select won't result in tile sizes that are too small or too large for your
application.

The `max_bytes_per_chunk` parameter can be used to cap the size of a chunk.
A minimum of 16 MiB is enforced, but no maximum, so if you are compressing you must ensure that you have sufficient
memory for all your chunks to be stored in memory at once.

#### Example

Suppose your frame size is 1920 x 1080 x 1, with a pixel type of unsigned 8-bit integer.
You can use a tile size of 640 x 360 x 1, which will divide your frame evenly into 9 tiles.
You want chunk sizes of at most 64 MiB.
You would configure your storage properties as follows:

```c
storage_properties_set_chunking_props(&storage_props,
                                      640,
                                      360,
                                      1,
                                      64 * 1024 * 1024);
```

Note that 64 * 1024 * 1024 / (640 * 360) = 291.2711111111111, so each chunk will contain 291 tiles, or about 63.94 MiB
raw, before compression.

### Compression

Compression is done via [Blosc][].
Supported codecs are **lz4** and **zstd**, which can be used by using the **ZarrBlosc1Lz4ByteShuffle** and
**ZarrBlosc1ZstdByteShuffle** devices, respectively.
For a comparison of these codecs, please refer to the [Blosc docs][].

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html

[Blosc]: https://github.com/Blosc/c-blosc

[Blosc docs]: https://www.blosc.org/
