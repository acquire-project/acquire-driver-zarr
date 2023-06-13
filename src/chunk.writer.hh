#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#ifdef __cplusplus

#include <vector>

#include <platform.h>

#include "zarr.encoder.hh"
#include "zarr.blosc.hh"
#include "tiled.frame.hh"

#ifdef min
#undef min
#undef max
#endif

namespace acquire::sink::zarr {

struct ChunkWriter final
{
  public:
    ChunkWriter() = delete;
    ChunkWriter(BaseEncoder* encoder,
                const ImageShape& image,
                const TileShape& tile,
                uint32_t layer,
                uint32_t tile_col,
                uint32_t tile_row,
                uint32_t tile_plane,
                size_t max_bytes_per_chunk);
    ~ChunkWriter();

    void set_dimension_separator(char separator);
    void set_base_directory(const std::string& base_directory);
    void open_chunk_file();
    void close_current_file();
    size_t write_frame(const std::shared_ptr<TiledFrame>& frame);

    const ImageShape& image_shape() const noexcept;
    const TileShape& tile_shape() const noexcept;

    std::mutex& mutex() noexcept;

    const uint32_t tile_col;
    const uint32_t tile_row;
    const uint32_t tile_plane;

  private:
    BaseEncoder* const encoder_{};

    size_t bytes_per_chunk_;
    size_t tiles_per_chunk_;
    size_t bytes_written_;

    std::string base_dir_;
    uint32_t layer_;
    int current_chunk_;
    char dimension_separator_;
    struct file* current_file_;

    std::optional<BloscCompressor> compressor_;

    std::mutex mutex_;
    ImageShape image_shape_;
    TileShape tile_shape_;

    std::vector<uint8_t> buffer_;

    size_t write(const uint8_t* beg, const uint8_t* end);
    void finalize_chunk();
    void rollover();
};
} // namespace acquire::sink::zarr
#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
