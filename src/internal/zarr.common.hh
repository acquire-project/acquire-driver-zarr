#pragma once

#include "stream.settings.hh"

using Dimension = ZarrDimension_s;

namespace zarr {
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
} // namespace zarr