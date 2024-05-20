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
    std::string data_root_;
    std::vector<Sink*> sinks_;

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
