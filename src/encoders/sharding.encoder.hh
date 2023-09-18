#ifndef H_ACQUIRE_STORAGE_SHARDING_ENCODER_V0
#define H_ACQUIRE_STORAGE_SHARDING_ENCODER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "chunking.encoder.hh"

#include <vector>

namespace acquire::sink::zarr {
struct ShardingEncoder final
{
  public:
    ShardingEncoder() = delete;
    ShardingEncoder(const ImageDims& image_dims,
                    const ImageDims& shard_dims,
                    const ImageDims& chunk_dims);
    ~ShardingEncoder();

    /// Encoder
    size_t encode(uint8_t* bytes_out,
                  size_t nbytes_out,
                  const uint8_t* bytes_in,
                  size_t nbytes_in) const;

  private:
    ImageDims outer_;
    ImageDims middle_;
    ImageDims inner_;

    ChunkingEncoder outer_encoder_;
    ChunkingEncoder inner_encoder_;

    mutable uint8_t* buf_;
    mutable size_t buf_size_;
};
}

#endif // H_ACQUIRE_STORAGE_SHARDING_ENCODER_V0
