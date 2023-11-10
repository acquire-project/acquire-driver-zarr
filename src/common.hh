#ifndef H_ACQUIRE_STORAGE_ZARR_COMMON_V0
#define H_ACQUIRE_STORAGE_ZARR_COMMON_V0

#include "logger.h"
#include "device/props/components.h"

#include <filesystem>
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

//#define TRACE(...) LOG(__VA_ARGS__)
#define TRACE(...)

#define containerof(ptr, T, V) ((T*)(((char*)(ptr)) - offsetof(T, V)))
#define countof(e) (sizeof(e) / sizeof(*(e)))

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct ImageDims
{
    uint32_t cols;
    uint32_t rows;

    friend bool operator<=(const ImageDims& lhs, const ImageDims& rhs) noexcept
    {
        return (lhs.cols <= rhs.cols) && (lhs.rows <= rhs.rows);
    }
};

struct ThreadPool {
        using JobT = std::function<bool(std::string&)>;
        struct ThreadContext
        {
            std::thread thread;
            std::mutex mutex;
            std::condition_variable cv;
            bool should_stop;
            bool ready;
        };
};

namespace common {
size_t
bytes_per_tile(const ImageDims& tile_shape, const SampleType& type);

size_t
frames_per_chunk(const ImageDims& tile_shape,
                 SampleType type,
                 uint64_t max_bytes_per_chunk);

size_t
bytes_per_chunk(const ImageDims& tile_shape,
                const SampleType& type,
                uint64_t max_bytes_per_chunk);

/// \brief Get the Zarr dtype for a given SampleType.
/// \param t An enumerated sample type.
/// \throw std::runtime_error if \par t is not a valid SampleType.
/// \return A representation of the SampleType \par t expected by a Zarr reader.
const char*
sample_type_to_dtype(SampleType t);

/// \brief Get a string representation of the SampleType enum.
/// \param t An enumerated sample type.
/// \return A human-readable representation of the SampleType \par t.
const char*
sample_type_to_string(SampleType t) noexcept;

/// \brief Write a string to a file.
/// @param path The path of the file to write.
/// @param str The string to write.
void
write_string(const std::string& path, const std::string& value);
} // namespace acquire::sink::zarr::common
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_COMMON_V0
