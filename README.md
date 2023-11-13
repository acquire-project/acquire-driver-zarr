# Acquire Zarr Driver

[![Build](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/build.yml)
[![Tests](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml/badge.svg)](https://github.com/acquire-project/acquire-driver-zarr/actions/workflows/test_pr.yml)
[![Chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://acquire-imaging.zulipchat.com/)

This is an Acquire Driver that supports chunked streaming to [zarr][].

## Devices

### Storage

- **Zarr**
- **ZarrBlosc1ZstdByteShuffle**
- **ZarrBlosc1Lz4ByteShuffle**
- **ZarrV3**
- **ZarrV3Blosc1ZstdByteShuffle**
- **ZarrV3Blosc1Lz4ByteShuffle**

## Using the Zarr storage device

Zarr has additional capabilities relative to the basic storage devices, namely _chunking_, _compression_, and
_multiscale storage_.

To compress while streaming, you can use one of the `ZarrBlosc1*` devices.
Chunking is configured using `storage_properties_set_chunking_props()` when configuring your video stream.
Multiscale storage can be enabled or disabled by calling `storage_properties_set_enable_multiscale()` when configuring
the video stream.

For the [Zarr v3] version of each device, you can use the `ZarrV3*` devices.
**Note:** Zarr v3 is not [yet](https://github.com/ome/ngff/pull/206) supported
by [ome-zarr-py](https://github.com/ome/ome-zarr-py), so you
will not be able to read multiscale metadata from the resulting dataset.

Zarr v3 *is* supported by [zarr-python](https://github.com/zarr-developers/zarr-python), but you will need to set two
environment variables to work with it:

```bash
export ZARR_V3_EXPERIMENTAL_API=1
export ZARR_V3_SHARDING=1
```

You can also set these variables in your Python script:

```python
import os

# these MUST come before importing zarr
os.environ["ZARR_V3_EXPERIMENTAL_API"] = "1"
os.environ["ZARR_V3_SHARDING"] = "1"

import zarr
```

### Configuring chunking

You can configure chunking by calling `storage_properties_set_chunking_props()` on your `StorageProperties` object
_after_ calling `storage_properties_init()`.
There are 3 parameters you can set to determine the chunk size, namely `chunk_width`, `chunk_height`,
and `chunk_planes`:

```c
int
storage_properties_set_chunking_props(struct StorageProperties* out,
                                      uint32_t chunk_width,
                                      uint32_t chunk_height,
                                      uint32_t chunk_planes)
```

| ![frames](https://github.com/aliddell/acquire-driver-zarr/assets/844464/3510d468-4751-4fa0-b2bf-0e29a5f3ea1c) |
|:-------------------------------------------------------------------------------------------------------------:|
|                                            A collection of frames.                                            |

A _tile_ is a contiguous section, or region of interest, of a _frame_.

| ![tiles](https://github.com/aliddell/acquire-driver-zarr/assets/844464/f8d16139-e0ac-44db-855f-2f5ef305c98b) |
|:------------------------------------------------------------------------------------------------------------:|
|                                 A collection of frames, divided into tiles.                                  |

A _chunk_ is nothing more than some number of stacked tiles from subsequent frames, with each tile in a chunk having
the same ROI in its respective frame.

| ![chunks](https://github.com/aliddell/acquire-driver-zarr/assets/844464/653e4d82-363e-4e04-9a42-927b052fb6e7) |
|:-------------------------------------------------------------------------------------------------------------:|
|            A collection of frames, divided into tiles. A single chunk has been highlighted in red.            |

You can specify the width and height, in pixels, of each tile.
If any of these values are unset (equivalently, set to 0), or if they are set to a value larger than the frame size,
the full value of the frame size along that dimension will be used instead.
You should take care that the values you select won't result in tile sizes that are too small or too large for your
application.
You can also set the number of tile *planes* to concatenate into a chunk.
If this value is unset (or set to 0), it will default to a prescribed minimum value of 32.

#### Example

Suppose your frame size is 1920 x 1080, with a pixel type of unsigned 8-bit integer.
You can use a tile size of 640 x 360, which will divide your frame evenly into 9 tiles.
You want chunk sizes of at most 32 MiB and this works out to 32 * 2^20 / (640 * 360) = 145.63555555555556, so you select
145 chunk planes.
You would configure your storage properties as follows:

```c
storage_properties_set_chunking_props(&storage_props,
                                      640,
                                      360,
                                      145);
```

### Configuring sharding

Configuring sharding is similar to configuring chunking.
You can configure sharding by calling `storage_properties_set_sharding_props()` on your `StorageProperties` object
_after_ calling `storage_properties_init()`.
There are 3 parameters you can set to determine the shard size, namely `shard_width`, `shard_height`,
and `shard_planes`.
**Note:** whereas the unit for the width, height, and plane values when chunking is *pixels*, when sharding, the unit is
*chunks*.
So in the previous example, if you wanted combine all your chunks together into a single shard, you would set your shard
properties like so:

```c
storage_properties_set_sharding_props(&storage_props,
                                      3, // width: 1920 / 640
                                      3, // height: 1080 / 360
                                      1);
```

This would result in all 9 chunks being combined into a single shard.

```c

### Compression

Compression is done via [Blosc][].
Supported codecs are **lz4** and **zstd**, which can be used by using the **ZarrBlosc1Lz4ByteShuffle** and
**ZarrBlosc1ZstdByteShuffle** devices, respectively.
For a comparison of these codecs, please refer to the [Blosc docs][].

### Configuring multiscale

In order to enable or disable multiscale storage for your video stream, you can call
`storage_properties_set_enable_multiscale()` on your `StorageProperties` object _after_ calling
`storage_properties_init()` on it.

```c
int
storage_properties_set_enable_multiscale(struct StorageProperties* out,
                                         uint8_t enable);
```

To enable, pass `1` as the `enable` parameter, and to disable, pass `0`.

If enabled, the Zarr writer will write a pyramid of frames, with each level of the pyramid halving each dimension of
the previous level, until the dimensions are less than or equal to a single tile.

#### Example

Suppose your frame size is 1920 x 1080, with a tile size of 384 x 216.
Then the sequence of levels will have dimensions 1920 x 1080, 960 x 540, 480 x 270, and 240 x 135.

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html

[Blosc]: https://github.com/Blosc/c-blosc

[Blosc docs]: https://www.blosc.org/

[Zarr v3]: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html