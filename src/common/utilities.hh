#ifndef H_ACQUIRE_STORAGE_ZARR_COMMON_V0
#define H_ACQUIRE_STORAGE_ZARR_COMMON_V0

#include "logger.h"
#include "device/props/components.h"
#include "device/props/storage.h"
#include "dimension.hh"

#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>

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
/// @return A representation of the SampleType @par t expected` by a Zarr reader.
const char*
sample_type_to_dtype(SampleType t);

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
} // namespace acquire::sink::zarr::common
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_COMMON_V0
