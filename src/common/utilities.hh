#ifndef H_ACQUIRE_STORAGE_ZARR_COMMON_UTILITIES_V0
#define H_ACQUIRE_STORAGE_ZARR_COMMON_UTILITIES_V0

#include "logger.h"
#include "device/props/components.h"
#include "device/props/storage.h"
#include "../internal/macros.hh"
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
