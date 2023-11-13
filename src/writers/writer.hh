#ifndef H_ACQUIRE_ZARR_WRITER_V0
#define H_ACQUIRE_ZARR_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "platform.h"
#include "device/props/components.h"

#include "../common.hh"
#include "blosc.compressor.hh"

#include <condition_variable>
#include <filesystem>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct Zarr;

struct FileCreator
{
    FileCreator() = delete;
    explicit FileCreator(std::shared_ptr<common::ThreadPool> thread_pool);
    ~FileCreator() noexcept = default;

    [[nodiscard]] bool create(const fs::path& base_dir,
                              int n_c,
                              int n_y,
                              int n_x,
                              std::vector<file>& files) noexcept;

  private:
    fs::path base_dir_;
    std::shared_ptr<common::ThreadPool> thread_pool_;

    bool create_c_dirs_(int n_c) noexcept;
    bool create_y_dirs_(int n_c, int n_y) noexcept;
};

struct Writer
{
  public:
    Writer() = delete;
    Writer(const ImageDims& frame_dims,
           const ImageDims& tile_dims,
           uint32_t frames_per_chunk,
           const std::string& data_root,
           std::shared_ptr<common::ThreadPool> thread_pool);

    /// Constructor with Blosc compression params
    Writer(const ImageDims& frame_dims,
           const ImageDims& tile_dims,
           uint32_t frames_per_chunk,
           const std::string& data_root,
           std::shared_ptr<common::ThreadPool> thread_pool,
           const BloscCompressionParams& compression_params);
    virtual ~Writer() noexcept = default;

    [[nodiscard]] bool write(const VideoFrame* frame);
    void finalize() noexcept;

    uint32_t frames_written() const noexcept;

  protected:
    /// Tiling/chunking
    ImageDims frame_dims_;
    ImageDims tile_dims_;
    uint16_t tiles_per_frame_x_;
    uint16_t tiles_per_frame_y_;
    SampleType pixel_type_;
    uint32_t frames_per_chunk_;
    std::vector<std::vector<uint8_t>> chunk_buffers_;

    /// Compression
    std::optional<BloscCompressionParams> blosc_compression_params_;

    /// Filesystem
    FileCreator file_creator_;
    fs::path data_root_;
    std::vector<file> files_;

    /// Multithreading
    std::mutex buffers_mutex_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t current_chunk_;
    std::shared_ptr<common::ThreadPool> thread_pool_;

    void validate_frame_(const VideoFrame* frame);

    virtual void make_buffers_() noexcept = 0; // FIXME: pull down

    void finalize_chunks_() noexcept;
    void compress_buffers_() noexcept;
    size_t write_frame_to_chunks_(const uint8_t* buf, size_t buf_size) noexcept;
    virtual void flush_() noexcept = 0;

    uint32_t tiles_per_frame_() const;

    /// Files
    [[nodiscard]] virtual bool make_files_() noexcept = 0; // FIXME: pull down
    void close_files_();
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
