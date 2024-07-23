#ifndef H_ACQUIRE_STORAGE_ZARR_V0
#define H_ACQUIRE_STORAGE_ZARR_V0

#include "device/kit/storage.h"

#include "common/utilities.hh"
#include "common/thread.pool.hh"
#include "common/s3.connection.hh"
#include "writers/writer.hh"
#include "writers/blosc.compressor.hh"

#include <filesystem>
#include <map>
#include <optional>
#include <stdexcept>
#include <utility> // std::pair
#include <vector>

#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace acquire::sink::zarr {
struct Zarr : public Storage
{
  public:
    Zarr();
    explicit Zarr(BloscCompressionParams&& compression_params);
    virtual ~Zarr() noexcept = default;

    /// Storage interface
    void set(const StorageProperties* props);
    void get(StorageProperties* props) const;
    virtual void get_meta(StoragePropertyMetadata* meta) const;
    void start();
    int stop() noexcept;
    size_t append(const VideoFrame* frames, size_t nbytes);
    size_t append_frame(const uint8_t* data,
                        size_t bytes_of_data,
                        const ImageShape& shape);
    void reserve_image_shape(const ImageShape* shape);

    /// Error state
    void set_error(const std::string& msg) noexcept;

  protected:
    /// static - set on construction
    std::optional<BloscCompressionParams> blosc_compression_params_;

    /// changes on set
    std::string dataset_root_;
    std::optional<std::string> s3_access_key_id_;
    std::optional<std::string> s3_secret_access_key_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    bool enable_multiscale_;

    /// changes on reserve_image_shape
    struct ImageShape image_shape_;
    std::vector<Dimension> acquisition_dimensions_;
    std::vector<std::shared_ptr<Writer>> writers_;

    /// changes on append
    // scaled frames, keyed by level-of-detail
    std::unordered_map<int, std::optional<VideoFrame*>> scaled_frames_;

    /// changes on start
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::shared_ptr<common::S3ConnectionPool> connection_pool_;

    /// changes on flush
    std::vector<std::unique_ptr<Sink>> metadata_sinks_;

    /// Multithreading
    mutable std::mutex mutex_; // for error_ / error_msg_

    /// Error state
    bool error_;
    std::string error_msg_;

    /// Setup
    void set_dimensions_(const StorageProperties* props);
    virtual void allocate_writers_() = 0;

    /// Metadata
    virtual void make_metadata_sinks_() = 0;

    // fixed metadata
    void write_fixed_metadata_() const;
    virtual void write_base_metadata_() const = 0;
    virtual void write_external_metadata_() const = 0;

    // mutable metadata, changes on flush
    void write_mutable_metadata_() const;
    virtual void write_group_metadata_() const = 0;
    virtual void write_array_metadata_(size_t level) const = 0;

    /// Multiscale
    json make_multiscale_metadata_() const;
    void write_multiscale_frames_(const uint8_t* data_,
                                  size_t bytes_of_data,
                                  const ImageShape& shape_);
};

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V0
