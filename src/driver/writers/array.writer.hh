#pragma once

#include "platform.h"
#include "device/props/components.h"

#include "common/dimension.hh"
#include "common/thread.pool.hh"
#include "common/s3.connection.hh"
#include "blosc.compressor.hh"
#include "file.sink.hh"

#include <condition_variable>
#include <filesystem>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct Zarr;

struct ArrayWriterConfig final
{
    ImageShape image_shape;
    std::vector<Dimension> dimensions;
    int level_of_detail;
    std::string dataset_root;
    std::optional<BloscCompressionParams> compression_params;
};

/// @brief Downsample the array writer configuration to a lower resolution.
/// @param[in] config The original array writer configuration.
/// @param[out] downsampled_config The downsampled array writer configuration.
/// @return True if @p downsampled_config can be downsampled further.
/// This is determined by the chunk size in @p config. This function will return
/// false if and only if downsampling brings one or more dimensions lower than
/// the chunk size along that dimension.
[[nodiscard]] bool
downsample(const ArrayWriterConfig& config,
           ArrayWriterConfig& downsampled_config);

struct ArrayWriter
{
  public:
    ArrayWriter() = delete;
    ArrayWriter(const ArrayWriterConfig& config,
                std::shared_ptr<common::ThreadPool> thread_pool,
                std::shared_ptr<common::S3ConnectionPool> connection_pool);

    virtual ~ArrayWriter() noexcept = default;

    [[nodiscard]] size_t write(const uint8_t* data, size_t bytes_of_frame);
    void finalize();

  protected:
    ArrayWriterConfig config_;

    /// Chunking
    std::vector<std::vector<uint8_t>> chunk_buffers_;

    /// Filesystem
    std::string data_root_;
    std::string meta_root_;
    std::vector<std::unique_ptr<Sink>> data_sinks_;
    std::unique_ptr<Sink> metadata_sink_;

    /// Multithreading
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::mutex buffers_mutex_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_finalizing_;

    std::shared_ptr<common::S3ConnectionPool> connection_pool_;

    void make_buffers_() noexcept;
    size_t write_frame_to_chunks_(const uint8_t* buf, size_t buf_size);
    bool should_flush_() const;
    void compress_buffers_() noexcept;
    void flush_();
    [[nodiscard]] virtual bool flush_impl_() = 0;
    [[nodiscard]] virtual bool write_array_metadata_() = 0;
    virtual bool should_rollover_() const = 0;
    void close_sinks_();
    void rollover_();
};
} // namespace acquire::sink::zarr
