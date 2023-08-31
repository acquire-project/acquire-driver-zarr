#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#include <optional>
#include <queue>
#include <vector>

#include "platform.h"

#include "zarr.encoder.hh"
#include "zarr.blosc.hh"
#include "tiled.frame.hh"

namespace {
struct FrameCmp
{
    bool operator()(
      const std::shared_ptr<acquire::sink::zarr::TiledFrame>& a,
      const std::shared_ptr<acquire::sink::zarr::TiledFrame>& b) const
    {
        return a->frame_id() > b->frame_id();
    }
};
}

namespace acquire::sink::zarr {

struct ChunkWriter final
{
  public:
    ChunkWriter() = delete;

    /// @param encoder Encoder to use for encoding data as it comes in.
    /// @param image_shape Shape and strides of the frame.
    /// @param tile_shape Dimensions of the tile.
    /// @param lod Multiscale level of detail. Full resolution is 0.
    /// @param tile_col Column index, in tile space, of this tile.
    /// @param tile_row Row index, in tile space, of this tile.
    /// @param tile_plane Plane index, in tile space, of this tile.
    /// @param max_bytes_per_chunk Maximum bytes per chunk.
    /// @param dimension_separator Separator to use between dimension names.
    /// @param base_directory Base directory to write chunks to.
    ChunkWriter(BaseEncoder* encoder,
                const ImageShape& image_shape,
                const TileShape& tile_shape,
                uint16_t lod,
                uint32_t tile_col,
                uint32_t tile_row,
                uint32_t tile_plane,
                uint64_t max_bytes_per_chunk,
                char dimension_separator,
                const std::string& base_directory);
    ~ChunkWriter();

    [[nodiscard]] bool push_frame(std::shared_ptr<TiledFrame> frame);

    const ImageShape& image_shape() const noexcept;
    const TileShape& tile_shape() const noexcept;

    uint32_t frames_written() const;

  private:
    BaseEncoder* const encoder_;

    const uint32_t tile_col_;
    const uint32_t tile_row_;
    const uint32_t tile_plane_;

    uint64_t bytes_per_chunk_;
    uint32_t tiles_per_chunk_;
    uint64_t bytes_written_;

    std::string base_dir_;
    uint16_t level_of_detail_;
    int current_chunk_;
    char dimension_separator_;
    std::optional<struct file> current_file_;

    std::optional<CompressionParams> compressor_;

    mutable std::mutex mutex_;
    ImageShape image_shape_;
    TileShape tile_shape_;

    std::priority_queue<std::shared_ptr<TiledFrame>,
                        std::vector<std::shared_ptr<TiledFrame>>,
                        FrameCmp>
      frames_;
    int64_t last_frame_;

    std::vector<uint8_t> buffer_;

    void open_chunk_file_();
    void close_current_file_();
    size_t write_(const uint8_t* beg, const uint8_t* end);
    void finalize_chunk_();
    void rollover_();
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
