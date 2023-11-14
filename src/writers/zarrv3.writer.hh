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
    ImageDims shard_dims_;
    uint16_t shards_per_frame_x_;
    uint16_t shards_per_frame_y_;

    uint16_t chunks_per_shard_() const;
    uint16_t shards_per_frame_() const;

    void flush_() noexcept override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V3_WRITER_V0
