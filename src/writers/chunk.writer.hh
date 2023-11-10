#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

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
struct ChunkWriter final : public Writer
{
  public:
    ChunkWriter() = delete;
    ChunkWriter(const ImageDims& frame_dims,
                const ImageDims& tile_dims,
                uint32_t frames_per_chunk,
                const std::string& data_root,
                Zarr* zarr);

    /// Constructor with Blosc compression params
    ChunkWriter(const ImageDims& frame_dims,
                const ImageDims& tile_dims,
                uint32_t frames_per_chunk,
                const std::string& data_root,
                Zarr* zarr,
                const BloscCompressionParams& compression_params);
    ~ChunkWriter() override = default;

  private:
    void make_buffers_() noexcept override;
    void flush_() noexcept override;
    [[nodiscard]] bool make_files_() noexcept override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
