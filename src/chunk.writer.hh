#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#include <optional>
#include <vector>

#include "platform.h"

#include "zarr.encoder.hh"
#include "zarr.blosc.hh"
#include "tiled.frame.hh"

namespace acquire::sink::zarr {

struct ChunkWriter final
{
  public:
    ChunkWriter() = delete;

    /// @param encoder Encoder to use for encoding data as it comes in.
    /// @param image_shape Shape and strides of the frame.
    /// @param tile_shape Dimensions of the tile.
    /// @param layer Multiscale layer. Full resolution is 0.
    /// @param tile_col Column index, in tile space, of this tile.
    /// @param tile_row Row index, in tile space, of this tile.
    /// @param tile_plane Plane index, in tile space, of this tile.
    /// @param max_bytes_per_chunk Maximum bytes per chunk.
    /// @param dimension_separator Separator to use between dimension names.
    /// @param base_directory Base directory to write chunks to.
    ChunkWriter(BaseEncoder* encoder,
                const ImageShape& image_shape,
                const TileShape& tile_shape,
                uint32_t layer,
                uint32_t tile_col,
                uint32_t tile_row,
                uint32_t tile_plane,
                uint64_t max_bytes_per_chunk,
                char dimension_separator,
                const std::string& base_directory);
    ~ChunkWriter();

    [[nodiscard]] bool write_frame(const TiledFrame& frame);

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
    uint32_t layer_;
    int current_chunk_;
    char dimension_separator_;
    std::optional<struct file> current_file_;

    std::optional<CompressionParams> compressor_;

    std::mutex mutex_;
    ImageShape image_shape_;
    TileShape tile_shape_;

    std::vector<uint8_t> buffer_;

    void open_chunk_file();
    void close_current_file();
    size_t write(const uint8_t* beg, const uint8_t* end);
    void finalize_chunk();
    void rollover();
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
