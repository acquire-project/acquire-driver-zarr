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

    /// @brief Create the directory structure for a Zarr v2 dataset.
    /// @param[in] base_dir The root directory for the dataset.
    /// @param[in] dimensions The dimensions of the dataset.
    /// @param[out] files The chunk files created.
    /// @return True iff the directory structure was created successfully.
    [[nodiscard]] bool create_chunk_files(
      const fs::path& base_dir,
      const std::vector<Dimension>& dimensions,
      std::vector<file>& files);

    /// @brief Create the directory structure for a Zarr v3 dataset.
    /// @param[in] base_dir The root directory for the dataset.
    /// @param[in] dimensions The dimensions of the dataset.
    /// @param[out] files The shard files created.
    /// @return True iff the directory structure was created successfully.
    [[nodiscard]] bool create_shard_files(
      const fs::path& base_dir,
      const std::vector<Dimension>& dimensions,
      std::vector<file>& files);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;

    /// @brief Parallel create a collection of directories.
    /// @param[in] dir_paths The directories to create.
    /// @return True iff all directories were created successfully.
    [[nodiscard]] bool make_dirs_(std::queue<fs::path>& dir_paths);

    /// @brief Parallel create a collection of files.
    /// @param[in,out] file_paths The files to create. Unlike `make_dirs_`,
    /// this function drains the queue.
    /// @param[out] files The files created.
    /// @return True iff all files were created successfully.
    [[nodiscard]] bool make_files_(std::queue<fs::path>& file_paths,
                                   std::vector<struct file>& files);
};

struct ArrayConfig
{
    ImageShape image_shape;
    std::vector<Dimension> dimensions;
    std::string data_root;
    std::optional<BloscCompressionParams> compression_params;
};

/// @brief Downsample the writer configuration to a lower resolution.
/// @param[in] config The original writer configuration.
/// @param[out] downsampled_config The downsampled writer configuration.
/// @return True if @p downsampled_config can be downsampled further.
/// This is determined by the chunk size in @p config. This function will return
/// false if and only if downsampling brings one or more dimensions lower than
/// the chunk size along that dimension.
[[nodiscard]] bool
downsample(const ArrayConfig& config, ArrayConfig& downsampled_config);

struct Writer
{
  public:
    Writer() = delete;
    Writer(const ArrayConfig& config,
           std::shared_ptr<common::ThreadPool> thread_pool);

    virtual ~Writer() noexcept = default;

    [[nodiscard]] bool write(const VideoFrame* frame);
    void finalize();

    const ArrayConfig& config() const noexcept;

    uint32_t frames_written() const noexcept;

  protected:
    ArrayConfig config_;

    /// Chunking
    std::vector<std::vector<uint8_t>> chunk_buffers_;

    /// Filesystem
    FileCreator file_creator_;
    fs::path data_root_;
    std::vector<file> files_;

    /// Multithreading
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::mutex buffers_mutex_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_finalizing_;

    void make_buffers_() noexcept;
    void validate_frame_(const VideoFrame* frame);
    size_t write_frame_to_chunks_(const uint8_t* buf, size_t buf_size);
    bool should_flush_() const;
    void compress_buffers_() noexcept;
    void flush_();
    [[nodiscard]] virtual bool flush_impl_() = 0;
    virtual bool should_rollover_() const = 0;
    void close_files_();
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
