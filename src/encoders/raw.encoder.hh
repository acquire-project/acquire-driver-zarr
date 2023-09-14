#ifndef H_ACQUIRE_STORAGE_RAW_ENCODER_V0
#define H_ACQUIRE_STORAGE_RAW_ENCODER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "encoder.hh"

namespace acquire::sink::zarr {
struct RawEncoder final
{
    RawEncoder() = default;
    ~RawEncoder() = default;

    /// Encoder
    size_t encode(uint8_t* bytes_out,
                  size_t nbytes_out,
                  uint8_t* bytes_in,
                  size_t nbytes_in) const;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_RAW_ENCODER_V0
