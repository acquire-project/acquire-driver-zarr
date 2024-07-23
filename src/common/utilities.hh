#ifndef H_ACQUIRE_STORAGE_ZARR_COMMON_UTILITIES_V0
#define H_ACQUIRE_STORAGE_ZARR_COMMON_UTILITIES_V0

#include "logger.h"
#include "device/props/components.h"
#include "device/props/storage.h"
#include "macros.hh"
#include "dimension.hh"

#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {

struct Zarr;

enum class ZarrVersion
{
    V2 = 2,
    V3
};

namespace common {
/// @brief Get the number of chunks along a dimension.
/// @param dimension A dimension.
/// @return The number of, possibly ragged, chunks along the dimension, given
/// the dimension's array and chunk sizes.
size_t
chunks_along_dimension(const Dimension& dimension);

/// @brief Get the number of chunks to hold in memory.
/// @param dimensions The dimensions of the array.
/// @return The number of chunks to buffer before writing out.
size_t
number_of_chunks_in_memory(const std::vector<Dimension>& dimensions);

/// @brief Get the number of shards along a dimension.
/// @param dimension A dimension.
/// @return The number of shards along the dimension, given the dimension's
/// array, chunk, and shard sizes.
size_t
shards_along_dimension(const Dimension& dimension);

/// @brief Get the number of shards to write at one time.
/// @param dimensions The dimensions of the array.
/// @return The number of shards to buffer and write out.
size_t
number_of_shards(const std::vector<Dimension>& dimensions);

/// @brief Get the number of chunks in a single shard.
/// @param dimensions The dimensions of the array.
/// @return The number of chunks in a shard.
size_t
chunks_per_shard(const std::vector<Dimension>& dimensions);

/// @brief Get the shard index for a given chunk index, given array dimensions.
/// @param chunk_index The index of the chunk.
/// @param dimensions The dimensions of the array.
/// @return The index of the shard containing the chunk.
size_t
shard_index_for_chunk(size_t chunk_index,
                      const std::vector<Dimension>& dimensions);

/// @brief Get the internal index of a chunk within a shard.
/// @param chunk_index The index of the chunk.
/// @param dimensions The dimensions of the array.
/// @return The index of the chunk within the shard.
size_t
shard_internal_index(size_t chunk_index,
                     const std::vector<Dimension>& dimensions);

/// @brief Get the size, in bytes, of a single chunk.
/// @param dimensions The dimensions of the array.
/// @param dtype The pixel type of the array.
/// @return The number of bytes to allocate for a chunk.
size_t
bytes_per_chunk(const std::vector<Dimension>& dimensions,
                const SampleType& dtype);

/// @brief Get a string representation of the SampleType enum.
/// @param t An enumerated sample type.
/// @return A human-readable representation of the SampleType @par t.
const char*
sample_type_to_string(SampleType t) noexcept;

/// @brief Align a size to a given alignment.
/// @param n Size to align.
/// @param align Alignment.
/// @return Aligned size.
size_t
align_up(size_t n, size_t align);

/// @brief Split a URI by the '/' delimiter.
/// @param uri String to split.
/// @return Vector of strings.
std::vector<std::string>
split_uri(const std::string& uri);

/// @brief Get the endpoint and bucket name from a URI.
/// @param[in] uri String to parse.
/// @param[out] endpoint The endpoint of the URI.
/// @param[out] bucket_name The bucket name of the URI.
void
parse_path_from_uri(std::string_view uri,
                    std::string& bucket_name,
                    std::string& path);

/// @brief Check if a URI is an S3 URI.
/// @param uri String to check.
/// @return True if the URI is an S3 URI, false otherwise.
bool
is_web_uri(std::string_view uri);
} // namespace acquire::sink::zarr::common
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_COMMON_UTILITIES_V0
