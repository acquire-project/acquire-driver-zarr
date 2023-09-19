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
                const std::string& data_root);

    /// Constructor with Blosc compression params
    ChunkWriter(const ImageDims& frame_dims,
                const ImageDims& tile_dims,
                uint32_t frames_per_chunk,
                const std::string& data_root,
                const BloscCompressionParams& compression_params);
    ~ChunkWriter() override = default;

    [[nodiscard]] bool write(const VideoFrame* frame) noexcept override;

  private:
    void make_buffers_() noexcept override;
    size_t write_bytes_(const uint8_t* buf, size_t buf_size) noexcept override;
    void compress_buffers_() noexcept;
    void flush_() noexcept override;
    void make_files_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
