# The Zarr Storage device

## Components

### The `StorageInterface` class.

Defines the interface that all Acquire `Storage` devices must implement, namely

- `set`: Set the storage properties.
- `get`: Get the storage properties.
- `get_meta`: Get metadata for the storage properties.
- `start`: Signal to the `Storage` device that it should start accepting frames.
- `stop`: Signal to the `Storage` device that it should stop accepting frames.
- `append`: Write incoming frames to the filesystem or other storage layer.
- `reserve_image_shape`: Set the image shape for allocating chunk writers.

### The `Zarr` class

An abstract class that implements the `StorageInterface`.
Zarr is "[a file storage format for chunked, compressed, N-dimensional arrays based on an open-source specification.](https://zarr.readthedocs.io/en/stable/index.html)"

### The `ZarrV2` class

Subclass of the `Zarr` class.
Implements abstract methods for writer allocation and metadata.
Specifically, `ZarrV2` allocates one writer of type `ChunkWriter` for each multiscale level-of-detail
and writes metadata in the format specified by the [Zarr V2 spec](https://zarr.readthedocs.io/en/stable/spec/v2.html).

### The `ZarrV3` class

Subclass of the `Zarr` class.
Implements abstract methods for writer allocation and metadata.
Specifically, `ZarrV3` allocates one writer of type `ShardWriter` for each multiscale level-of-detail
and writes metadata in the format specified by the [Zarr V3 spec](https://zarr-specs.readthedocs.io/en/latest/specs.html).

### The `Writer` class

An abstract class that writes frames to the filesystem or other storage layer.
In general, frames are chunked and potentially compressed.
The `Writer` handles chunking, chunk compression, and writing.

### The `ChunkWriter` class

Subclass of the `Writer` class.
Implements abstract methods relating to writing and flushing chunk buffers.
Chunk buffers, whether raw or compressed, are written to individual chunk files. 

### The `ShardWriter` class

Subclass of the `Writer` class.
Implements abstract methods relating to writing, sharding, and flushing chunk buffers.
Chunk buffers, whether raw or compressed, are concatenated into shards, which are written out to individual shard files.



### The `BloscCompressionParams` struct

Stores parameters for compression using [C-Blosc](https://github.com/Blosc/c-blosc).
