#ifndef H_ACQUIRE_STORAGE_CHUNKING_ENCODER_V0
#define H_ACQUIRE_STORAGE_CHUNKING_ENCODER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "encoder.hh"

#include <vector>

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

struct ChunkingEncoder final
{
  public:
    ChunkingEncoder() = delete;
    ChunkingEncoder(const ImageDims& frame_dims, const ImageDims& tile_dims);
    ~ChunkingEncoder() = default;

    /// Encoder
    size_t encode(uint8_t* bytes_out,
                  size_t nbytes_out,
                  const uint8_t* bytes_in,
                  size_t nbytes_in) const;

  private:
    ImageDims outer_;
    ImageDims inner_;

    std::vector<uint8_t> buf_;
};
}

#endif // H_ACQUIRE_STORAGE_CHUNKING_ENCODER_V0
