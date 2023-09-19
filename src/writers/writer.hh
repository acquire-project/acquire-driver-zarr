#ifndef H_ACQUIRE_ZARR_WRITER_V0
#define H_ACQUIRE_ZARR_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "platform.h"
#include "device/props/components.h"

#include "../common.hh"

#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct Writer
{
  public:
    struct ThreadContext
    {
        std::thread thread;
        std::mutex mutex;
        std::condition_variable cv;
        bool should_stop;
    };

    struct JobContext
    {
        uint8_t* buf;
        size_t buf_size;
        file* fh;
        uint64_t offset;
    };

    Writer() = delete;
    Writer(const ImageDims& frame_dims,
           const ImageDims& tile_dims,
           uint32_t frames_per_chunk,
           const std::string& data_root);
    virtual ~Writer();

    [[nodiscard]] virtual bool write(const VideoFrame* frame) noexcept = 0;
    void finalize() noexcept;

    uint32_t frames_written() const noexcept;

  protected:
    ImageDims frame_dims_;
    ImageDims tile_dims_;

    /// Tiling of the frame. The product is the number of tiles in a frame.
    uint16_t tile_cols_;
    uint16_t tile_rows_;
    SampleType pixel_type_;

    fs::path data_root_;
    std::vector<file> files_;

    uint32_t frames_per_chunk_;
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t t_;

    std::vector<std::vector<uint8_t>> buffers_;
    std::vector<ThreadContext> threads_;
    std::queue<JobContext> jobs_;
    std::mutex mutex_;

    std::optional<JobContext> pop_from_job_queue() noexcept;
    void worker_thread_(ThreadContext* ctx);

    [[nodiscard]] bool validate_frame_(const VideoFrame* frame) noexcept;

    virtual void make_buffers_() noexcept = 0;

    void finalize_chunks_() noexcept;
    virtual size_t write_bytes_(const uint8_t* buf, size_t buf_size) noexcept = 0;
    virtual void flush_() noexcept = 0;

    /// Files
    void make_files_();
    void close_files_();
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
