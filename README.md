# Acquire Zarr Driver

This is an Acquire Driver that supports chunked streaming to [zarr][].

## Devices

### Storage

- **Zarr**

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

### Configuring compression

Compression is done via [Blosc][].
As with chunking, you can configure compression by calling `storage_properties_set_compression_props()` on your
Zarr `Storage` object _after_ calling `storage_properties_init()`.
The 3 parameters you can set to determine compression behavior are `codec_id`, `clevel` (compression level),
and `shuffle`:

```c
storage_properties_set_compression_props(struct StorageProperties* out,
                                         const char* codec_id,
                                         size_t bytes_of_codec_id,
                                         int clevel,
                                         int shuffle)
```

Supported values for `clevel` are integers 1-9, corresponding to the various compression levels supported by Blosc. 
Supported values for `shuffle` are the integers 0 (no shuffle), 1 (byte shuffle), and 2 (bitshuffle).
Supported values for `codec_id` are:

- "blosclz"
- "lz4"
- "lz4hc"
- "zlib"
- "zstd"

For a comparison of these codecs, please refer to the [Blosc docs][Blosc].

[zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html

[Blosc]: https://github.com/Blosc/c-blosc
