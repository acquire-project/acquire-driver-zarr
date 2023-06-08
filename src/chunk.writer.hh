#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#ifdef __cplusplus

#include <condition_variable>
#include <queue>
#include <unordered_set>
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
    ChunkWriter(const ImageShape& image,
                const TileShape& tile,
                uint32_t tile_col,
                uint32_t tile_row,
                uint32_t tile_plane,
                size_t max_bytes_per_chunk,
                BaseEncoder* encoder);
    ~ChunkWriter();

    void set_dimension_separator(char separator);
    void set_base_directory(const std::string& base_directory);
    void open_chunk_file();
    void close_current_file();

    std::mutex& mutex() noexcept;
    const ImageShape& image_shape() const noexcept;
    const TileShape& tile_shape() const noexcept;

    /**************************
     * For use by Zarr writer *
     **************************/
    void push_frame(const TiledFrame* frame);
    [[nodiscard]] bool has_frame(uint64_t frame_id) const;
    [[nodiscard]] size_t active_frames() const;

    /**********************************
     * For use in chunk writer thread *
     **********************************/
    [[nodiscard]] const TiledFrame* pop_frame_and_make_current();
    void release_current_frame();
    size_t write(const uint8_t* beg, const uint8_t* end);

    const uint32_t tile_col;
    const uint32_t tile_row;
    const uint32_t tile_plane;

  private:
    BaseEncoder* const encoder_{};

    size_t bytes_per_chunk_;
    size_t tiles_per_chunk_;
    size_t bytes_written_;

    std::string base_dir_;
    int current_chunk_;
    char dimension_separator_;
    struct file* current_file_;

    std::queue<const TiledFrame*> frame_ptrs_;
    std::unordered_set<uint64_t> frame_ids_;
    std::optional<uint64_t> current_frame_id_;
    std::optional<BloscCompressor> compressor_;

    std::mutex mutex_;
    ImageShape image_shape_;
    TileShape tile_shape_;

    void finalize_chunk();
    void rollover();
};

struct WriterContext final
{
    ChunkWriter* writer;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop;
};

void
chunk_write_thread(WriterContext* context);

} // namespace acquire::sink::zarr
#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
