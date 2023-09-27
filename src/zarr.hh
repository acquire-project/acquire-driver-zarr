#ifndef H_ACQUIRE_STORAGE_ZARR_V0
#define H_ACQUIRE_STORAGE_ZARR_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "device/kit/storage.h"

#include "prelude.h"
#include "common.hh"
#include "writers/writer.hh"
#include "writers/blosc.compressor.hh"

#include <filesystem>
#include <map>
#include <optional>
#include <stdexcept>
#include <utility> // std::pair
#include <vector>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
// StorageInterface

struct StorageInterface : public Storage
{
    StorageInterface();
    virtual ~StorageInterface() = default;
    virtual void set(const StorageProperties* props) = 0;
    virtual void get(StorageProperties* props) const = 0;
    virtual void get_meta(StoragePropertyMetadata* meta) const = 0;
    virtual void start() = 0;
    virtual int stop() noexcept = 0;

    /// @return number of consumed bytes
    virtual size_t append(const VideoFrame* frames, size_t nbytes) = 0;

    /// @brief Set the image shape for allocating chunk writers.
    virtual void reserve_image_shape(const ImageShape* shape) = 0;
};

struct Zarr : StorageInterface
{
  public:
    using JobT = std::function<bool(std::string&)>;
    struct ThreadContext
    {
        std::thread thread;
        std::mutex mutex;
        std::condition_variable cv;
        bool should_stop;
        bool ready;
    };

    Zarr();
    Zarr(BloscCompressionParams&& compression_params);
    ~Zarr() noexcept override;

    /// StorageInterface
    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void start() override;
    int stop() noexcept override;
    size_t append(const VideoFrame* frames, size_t nbytes) override;
    void reserve_image_shape(const ImageShape* shape) override;

    /// Error state
    void set_error(const std::string& msg) noexcept;

    /// Multithreading
    void push_to_job_queue(JobT&& job);
    size_t jobs_on_queue() const;

  protected:
    using ChunkingProps = StorageProperties::storage_properties_chunking_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    /// static - set on construction
    std::optional<BloscCompressionParams> blosc_compression_params_;

    /// changes on set
    fs::path dataset_root_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    uint64_t max_bytes_per_chunk_;
    bool enable_multiscale_;

    /// changes on reserve_image_shape
    std::vector<std::pair<ImageDims, ImageDims>> image_tile_shapes_;
    SampleType pixel_type_;
    std::vector<std::shared_ptr<Writer>> writers_;

    /// changes on append
    // scaled frames, keyed by level-of-detail
    std::unordered_map<int, std::optional<VideoFrame*>> scaled_frames_;

    /// Multithreading
    std::vector<ThreadContext> threads_;
    mutable std::mutex mutex_; // for jobs_ and error_ / error_msg_
    std::queue<JobT> jobs_;

    /// Error state
    bool error_;
    std::string error_msg_;

    /// Setup
    void set_chunking(const ChunkingProps& props, const ChunkingMeta& meta);
    virtual void allocate_writers_() = 0;

    /// Metadata
    void write_all_array_metadata_() const;
    virtual void write_array_metadata_(size_t level) const = 0;
    virtual void write_external_metadata_() const = 0;
    virtual void write_base_metadata_() const = 0;
    virtual void write_group_metadata_() const = 0;

    /// Filesystem
    virtual fs::path get_data_directory_() const = 0;

    /// Multiscale
    void write_multiscale_frames_(const VideoFrame* frame);

    /// Multithreading
    std::optional<JobT> pop_from_job_queue_() noexcept;
    void worker_thread_(ThreadContext* ctx);
};

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V0
