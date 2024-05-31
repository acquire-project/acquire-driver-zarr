#ifndef H_ACQUIRE_STORAGE_ZARR_COMMON_V0
#define H_ACQUIRE_STORAGE_ZARR_COMMON_V0

#include "logger.h"
#include "device/props/components.h"
#include "device/props/storage.h"

#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#define LOG(...) aq_logger(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) aq_logger(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Expression was false: " #e);             \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

// #define TRACE(...) LOG(__VA_ARGS__)
#define TRACE(...)

#define containerof(ptr, T, V) ((T*)(((char*)(ptr)) - offsetof(T, V)))
#define countof(e) (sizeof(e) / sizeof(*(e)))

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct Dimension
{
  public:
    explicit Dimension(const std::string& name,
                       DimensionType kind,
                       uint32_t array_size_px,
                       uint32_t chunk_size_px,
                       uint32_t shard_size_chunks);
    explicit Dimension(const StorageDimension& dim);

    const std::string name;
    const DimensionType kind;
    const uint32_t array_size_px;
    const uint32_t chunk_size_px;
    const uint32_t shard_size_chunks;
};

struct Zarr;

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

/// @brief Get the Zarr dtype for a given SampleType.
/// @param t An enumerated sample type.
/// @throw std::runtime_error if @par t is not a valid SampleType.
/// @return A representation of the SampleType @par t expected by a Zarr reader.
const char*
sample_type_to_dtype(SampleType t);

/// @brief Get a string representation of the SampleType enum.
/// @param t An enumerated sample type.
/// @return A human-readable representation of the SampleType @par t.
const char*
sample_type_to_string(SampleType t) noexcept;

/// @brief Split a URI by the '/' delimiter.
/// @param uri String to split.
/// @return Vector of strings.
std::vector<std::string>
split_uri(const std::string& uri);

/// @brief Check if a URI is an S3 URI.
/// @param uri String to check.
/// @return True if the URI is an S3 URI, false otherwise.
bool
is_s3_uri(const std::string& uri);
} // namespace acquire::sink::zarr::common
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_COMMON_V0
