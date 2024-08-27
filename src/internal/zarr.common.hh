#pragma once

#include "stream.settings.hh"
#include "zarr.h"

namespace zarr {
using Dimension = ZarrDimension_s;

/**
 * @brief Get the number of bytes for a given data type.
 * @param data_type The data type.
 * @return The number of bytes for the data type.
 */
size_t
bytes_of_data_type(ZarrDataType data_type);

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
} // namespace zarr