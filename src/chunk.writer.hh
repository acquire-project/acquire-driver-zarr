#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#ifdef __cplusplus

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

using thread_t = thread;

struct ChunkWriter final
{
  public:
    ChunkWriter() = delete;
    ChunkWriter(const FrameROI& roi, size_t bytes_per_chunk,
                BaseEncoder* encoder);
    ~ChunkWriter();

    [[nodiscard]] FrameROI& roi();
    [[nodiscard]] size_t bytes_per_tile() const;
    [[nodiscard]] size_t bytes_per_chunk() const;
    [[nodiscard]] size_t tiles_written() const;
    [[nodiscard]] size_t bytes_written() const;

    void set_dimension_separator(char separator);
    void set_base_directory(const std::string& base_directory);
    void set_current_chunk_file();
    void close_current_file();

    /**************************
     * For use by Zarr writer *
     **************************/
    void push_frame(const TiledFrame* frame);
    [[nodiscard]] bool has_frame(uint64_t frame_id) const;
    [[nodiscard]] bool has_thread() const;
    [[nodiscard]] size_t active_frames() const;

    void assign_thread(thread_t** t);
    thread_t* release_thread();

    /**********************************
     * For use in chunk writer thread *
     **********************************/
    [[nodiscard]] const TiledFrame* pop_frame_and_make_current();
    void release_current_frame();
    [[nodiscard]] bool should_wait_for_work() const;
    size_t write(const uint8_t* beg, const uint8_t* end);

  private:
    BaseEncoder* encoder_{};

    FrameROI roi_;
    size_t bytes_per_chunk_;
    size_t tiles_per_chunk_;
    size_t bytes_written_;

    std::string base_dir_;
    size_t current_chunk_;
    char dimension_separator_;

    mutable lock lock_;
    thread_t* thread_;
    std::queue<const TiledFrame*> frame_ptrs_;
    std::unordered_set<uint64_t> frame_ids_;
    std::optional<uint64_t> current_frame_id_;
    std::optional<BloscCompressor> compressor_;
    bool should_wait_for_work_;

    void update_current_chunk_file();
    void finalize_chunk();
    void rollover();
};
} // namespace acquire::sink::zarr
#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
