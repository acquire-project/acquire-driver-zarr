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

    [[nodiscard]] bool create_chunk_files(
      const fs::path& base_dir,
      const std::vector<Dimension>& dimensions,
      std::vector<file>& files);
    [[nodiscard]] bool create_shard_files(
      const fs::path& base_dir,
      const std::vector<Dimension>& dimensions,
      std::vector<file>& files);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;

    [[nodiscard]] bool make_dirs_(std::queue<fs::path>& dir_paths);
    [[nodiscard]] bool make_files_(std::queue<fs::path>& file_paths,
                                   std::vector<struct file>& files);
};

struct WriterConfig
{
    ImageShape image_shape;
    std::vector<Dimension> dimensions;
    std::string data_root;
    std::optional<BloscCompressionParams> compression_params;
};

std::optional<struct WriterConfig>
downsample(const WriterConfig& config);

struct Writer
{
  public:
    Writer() = delete;
    Writer(const WriterConfig& config,
           std::shared_ptr<common::ThreadPool> thread_pool);

    virtual ~Writer() noexcept = default;

    [[nodiscard]] bool write(const VideoFrame* frame);
    void finalize();

    const WriterConfig& config() const noexcept;

    uint32_t frames_written() const noexcept;

  protected:
    WriterConfig config_;

    /// Chunking
    std::vector<std::vector<uint8_t>> chunk_buffers_;

    /// Filesystem
    FileCreator file_creator_;
    fs::path data_root_;
    std::vector<file> files_;

    /// Multithreading
    std::mutex buffers_mutex_;

    /// Bookkeeping
    std::vector<uint64_t> chunks_per_dim_;
    std::vector<uint64_t> chunk_strides_;

    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t current_chunk_;
    std::shared_ptr<common::ThreadPool> thread_pool_;

    void validate_frame_(const VideoFrame* frame);

    void make_buffers_() noexcept;

    void finalize_chunks_() noexcept;
    void compress_buffers_() noexcept;
    size_t write_frame_to_chunks_(const uint8_t* buf, size_t buf_size);

    /// Files
    virtual bool should_flush_() const = 0;
    void flush_();
    [[nodiscard]] virtual bool flush_impl_() = 0;
    void close_files_();
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
