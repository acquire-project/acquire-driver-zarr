#ifndef H_ACQUIRE_ZARR_WRITER_V0
#define H_ACQUIRE_ZARR_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "platform.h"
#include "device/props/components.h"

#include "../common.hh"
#include "blosc.compressor.hh"
#include "sink.hh"

#include <condition_variable>
#include <filesystem>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct Zarr;

struct WriterConfig
{
    ImageShape image_shape;
    std::vector<Dimension> dimensions;
    std::string dataset_root;
    size_t array_index;
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
downsample(const WriterConfig& config, WriterConfig& downsampled_config);

struct Writer
{
  public:
    Writer() = delete;
    Writer(const WriterConfig& config,
           std::shared_ptr<common::ThreadPool> thread_pool);

    virtual ~Writer() noexcept = default;

    [[nodiscard]] bool write(const VideoFrame* frame);

    [[nodiscard]] virtual bool write_base_metadata(
      const std::string& metadata) = 0;
    [[nodiscard]] virtual bool write_external_metadata(
      const std::string& metadata) = 0;

    [[nodiscard]] virtual bool write_group_metadata(
      const std::string& metadata) = 0;
    [[nodiscard]] virtual bool write_array_metadata() = 0;

    void finalize();

    const WriterConfig& config() const noexcept;
    uint32_t frames_written() const noexcept;

  protected:
    WriterConfig writer_config_;

    /// Chunking
    std::vector<std::vector<uint8_t>> chunk_buffers_;

    /// Multithreading
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::mutex buffers_mutex_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_finalizing_;

    virtual std::string make_array_metadata_() const = 0;

    void make_buffers_() noexcept;
    void validate_frame_(const VideoFrame* frame);
    size_t write_frame_to_chunks_(const uint8_t* buf, size_t buf_size);
    bool should_flush_() const;
    void compress_buffers_() noexcept;
    void flush_();
    [[nodiscard]] virtual bool flush_impl_() = 0;
    virtual bool should_rollover_() const = 0;
    virtual void close_() = 0;
    void rollover_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_WRITER_V0
