#ifndef H_ACQUIRE_ZARR_V3_WRITER_V0
#define H_ACQUIRE_ZARR_V3_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "writer.hh"

#include "platform.h"
#include "device/props/components.h"

#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct ZarrV3Writer final : public Writer
{
  public:
    ZarrV3Writer() = delete;

    ZarrV3Writer(const ImageShape& image_shape,
                 const ChunkShape& chunk_shape,
                 const ShardShape& shard_shape,
                 const std::string& data_root,
                 std::shared_ptr<common::ThreadPool> thread_pool);

    ZarrV3Writer(const ImageShape& image_shape,
                 const ChunkShape& chunk_shape,
                 const ShardShape& shard_shape,
                 const std::string& data_root,
                 std::shared_ptr<common::ThreadPool> thread_pool,
                 const BloscCompressionParams& compression_params);

    ZarrV3Writer(const ImageDims& frame_dims,
                 const ImageDims& shard_dims,
                 const ImageDims& tile_dims,
                 uint32_t frames_per_chunk,
                 const std::string& data_root,
                 std::shared_ptr<common::ThreadPool> thread_pool);

    /// Constructor with Blosc compression params
    ZarrV3Writer(const ImageDims& frame_dims,
                 const ImageDims& shard_dims,
                 const ImageDims& tile_dims,
                 uint32_t frames_per_chunk,
                 const std::string& data_root,
                 std::shared_ptr<common::ThreadPool> thread_pool,
                 const BloscCompressionParams& compression_params);
    ~ZarrV3Writer() override = default;

  private:
    ShardShape shard_shape_;
    ImageDims shard_dims_;

    uint16_t chunks_per_shard_() const;
    uint16_t shards_per_frame_() const;
    uint16_t shards_in_x_() const;
    uint16_t shards_in_y_() const;

    void flush_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V3_WRITER_V0
