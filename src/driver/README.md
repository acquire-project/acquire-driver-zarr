# The Zarr Storage device

## Components

### The `Zarr` class

An abstract class that implements the `Storage` device interface.
Zarr
is "[a file storage format for chunked, compressed, N-dimensional arrays based on an open-source specification.](https://zarr.readthedocs.io/en/stable/index.html)"

### The `ZarrV2` class

Subclass of the `Zarr` class.
Implements abstract methods for writer allocation and metadata.
Specifically, `ZarrV2` allocates one writer of type `ZarrV2ArrayWriter` for each multiscale level-of-detail
and writes metadata in the format specified by the [Zarr V2 spec](https://zarr.readthedocs.io/en/stable/spec/v2.html).

### The `ZarrV3` class

Subclass of the `Zarr` class.
Implements abstract methods for writer allocation and metadata.
Specifically, `ZarrV3` allocates one writer of type `ZarrV3ArrayWriter` for each multiscale level-of-detail
and writes metadata in the format specified by
the [Zarr V3 spec](https://zarr-specs.readthedocs.io/en/latest/specs.html).

### The `ArrayWriter` class

An abstract class that writes frames to the filesystem or other storage layer.
In general, frames are chunked and potentially compressed.
The `ArrayWriter` handles chunking, chunk compression, and writing.

### The `ZarrV2ArrayWriter` class

Subclass of the `ArrayWriter` class.
Implements abstract methods relating to writing and flushing chunk buffers.
Chunk buffers, whether raw or compressed, are written to individual chunk files.

### The `ZarrV3ArrayWriter` class

Subclass of the `ArrayWriter` class.
Implements abstract methods relating to writing, sharding, and flushing chunk buffers.
Chunk buffers, whether raw or compressed, are concatenated into shards, which are written out to individual shard files.

### The `BloscCompressionParams` struct

Stores parameters for compression using [C-Blosc](https://github.com/Blosc/c-blosc).
