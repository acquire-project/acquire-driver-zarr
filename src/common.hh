#ifndef ACQUIRE_DRIVER_ZARR_COMMON_H
#define ACQUIRE_DRIVER_ZARR_COMMON_H

#include "prelude.h"

#include "device/props/components.h"

#include <filesystem>

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

namespace common {
size_t
bytes_of_type(const SampleType& type);

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

#endif // ACQUIRE_DRIVER_ZARR_COMMON_H
