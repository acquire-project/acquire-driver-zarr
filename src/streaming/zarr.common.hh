#pragma once

#include "stream.settings.hh"
#include "zarr.stream.hh"

#include "zarr.h"

namespace zarr {
using Dimension = ZarrDimension_s;

/**
 * @brief Get the number of chunks along a dimension.
 * @param dimension A dimension.
 * @return The number of, possibly ragged, chunks along the dimension, given
 * the dimension's array and chunk sizes.
 */
size_t
chunks_along_dimension(const Dimension& dimension);

/**
 * @brief Get the number of shards along a dimension.
 * @param dimension A dimension.
 * @return The number of shards along the dimension, given the dimension's
 * array, chunk, and shard sizes.
 */
size_t
shards_along_dimension(const Dimension& dimension);

/**
 * @brief Get the index of a chunk in the chunk lattice for a given frame and
 * dimension.
 * @param frame_id The frame ID.
 * @param dimension_idx The index of the dimension in the dimension vector.
 * @param dims The dimensions.
 * @return The index of the chunk in the chunk lattice.
 */
size_t
chunk_lattice_index(size_t frame_id,
                    size_t dimension_idx,
                    const std::vector<Dimension>& dims);

/**
 * @brief Find the offset in the array of chunk buffers for the given frame.
 * @param frame_id The frame ID.
 * @param dims The dimensions of the array.
 * @return The offset in the array of chunk buffers.
 */
size_t
tile_group_offset(size_t frame_id, const std::vector<Dimension>& dims);

/**
 * @brief Find the byte offset inside a chunk for a given frame and data type.
 * @param frame_id The frame ID.
 * @param dims The dimensions of the array.
 * @param type The data type of the array.
 * @return The byte offset inside a chunk.
 */
size_t
chunk_internal_offset(size_t frame_id,
                      const std::vector<Dimension>& dims,
                      ZarrDataType type);

/**
 * @brief Get the number of chunks to hold in memory.
 * @param dimensions The dimensions of the array.
 * @return The number of chunks to buffer before writing out.
 */
size_t
number_of_chunks_in_memory(const std::vector<Dimension>& dimensions);

/**
 * @brief Get the size, in bytes, of a single raw chunk.
 * @param dimensions The dimensions of the array.
 * @param type The data type of the array.
 * @return The number of bytes to allocate for a chunk.
 */
size_t
bytes_per_chunk(const std::vector<Dimension>& dimensions,
                ZarrDataType type);

/**
 * @brief Get the number of shards to write at one time.
 * @param dimensions The dimensions of the array.
 * @return The number of shards to buffer and write out.
 */
size_t
number_of_shards(const std::vector<Dimension>& dimensions);

/**
 * @brief Get the number of chunks in a single shard.
 * @param dimensions The dimensions of the array.
 * @return The number of chunks in a shard.
 */
size_t
chunks_per_shard(const std::vector<Dimension>& dimensions);

/**
 * @brief Get the shard index for a given chunk index, given array dimensions.
 * @param chunk_index The index of the chunk.
 * @param dimensions The dimensions of the array.
 * @return The index of the shard containing the chunk.
 */
size_t
shard_index_for_chunk(size_t chunk_index,
                      const std::vector<Dimension>& dimensions);

/**
 * @brief Get the streaming index of a chunk within a shard.
 * @param chunk_index The index of the chunk.
 * @param dimensions The dimensions of the array.
 * @return The index of the chunk within the shard.
 */
size_t
shard_internal_index(size_t chunk_index,
                     const std::vector<Dimension>& dimensions);
} // namespace zarr