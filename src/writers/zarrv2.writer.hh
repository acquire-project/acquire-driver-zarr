#ifndef H_ACQUIRE_ZARR_V2_WRITER_V0
#define H_ACQUIRE_ZARR_V2_WRITER_V0

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
struct ZarrV2Writer final : public Writer
{
  public:
    ZarrV2Writer() = delete;
    ZarrV2Writer(const ImageDims& frame_dims,
                const ImageDims& tile_dims,
                uint32_t frames_per_chunk,
                const std::string& data_root,
                std::shared_ptr<common::ThreadPool> thread_pool);

    /// Constructor with Blosc compression params
    ZarrV2Writer(const ImageDims& frame_dims,
                const ImageDims& tile_dims,
                uint32_t frames_per_chunk,
                const std::string& data_root,
                std::shared_ptr<common::ThreadPool> thread_pool,
                const BloscCompressionParams& compression_params);
    ~ZarrV2Writer() override = default;

  private:
    void flush_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V2_WRITER_V0
