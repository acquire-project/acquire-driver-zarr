#ifndef H_ACQUIRE_ZARR_CHUNK_WRITER_V0
#define H_ACQUIRE_ZARR_CHUNK_WRITER_V0

#ifdef __cplusplus

#include <queue>
#include <thread>
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
    ChunkWriter(const FrameROI& roi,
                size_t bytes_per_chunk,
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

    std::mutex& mutex() noexcept;

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
    [[nodiscard]] bool should_wait_for_work() const;
    size_t write(const uint8_t* beg, const uint8_t* end);

  private:
    BaseEncoder* encoder_{};

    FrameROI roi_;
    size_t bytes_per_chunk_;
    size_t tiles_per_chunk_;
    size_t bytes_written_;

    std::string base_dir_;
    int current_chunk_;
    char dimension_separator_;

    std::queue<const TiledFrame*> frame_ptrs_;
    std::unordered_set<uint64_t> frame_ids_;
    std::optional<uint64_t> current_frame_id_;
    std::optional<BloscCompressor> compressor_;
    bool should_wait_for_work_;

    std::mutex mutex_;

    void update_current_chunk_file();
    void finalize_chunk();
    void rollover();
};

struct WriterContext final
{
    ChunkWriter* writer;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop;
    //    std::thread thread;
    //    std::atomic<bool> is_running;
};

void
chunk_write_thread(WriterContext* context);

} // namespace acquire::sink::zarr
#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_CHUNK_WRITER_V0
