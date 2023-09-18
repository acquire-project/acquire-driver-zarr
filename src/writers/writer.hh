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
        Writer* writer;
        std::mutex mutex;
        std::condition_variable cv;
        bool should_stop;
    };

    struct JobContext
    {
        uint8_t* buf;
        size_t buf_size;
        file* file;
        uint64_t offset;
    };

    Writer() = default;
    Writer(const ImageDims& frame_dims,
           const ImageDims& tile_dims,
           uint32_t frames_per_chunk,
           const std::string& data_root);
    virtual ~Writer();

    std::optional<JobContext> pop_from_job_queue() noexcept;

    uint32_t frames_written() const noexcept;

  protected:
    ImageDims frame_dims_;
    ImageDims tile_dims_;

    /// Tiling of the frame. The product is the number of tiles in a frame.
    uint16_t tile_cols_;
    uint16_t tile_rows_;

    fs::path data_root_;
    std::vector<file> files_;

    uint32_t frames_per_chunk_;
    uint32_t frames_written_;

    std::vector<ThreadContext> threads_;
    std::queue<JobContext> jobs_;
    std::mutex mutex_;

    [[nodiscard]] bool validate_frame(const VideoFrame* frame) const noexcept;
    virtual void flush() noexcept = 0;

    /// Files
    void make_files_();
    void close_files_();
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
