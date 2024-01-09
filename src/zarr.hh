#ifndef H_ACQUIRE_STORAGE_ZARR_V0
#define H_ACQUIRE_STORAGE_ZARR_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "device/kit/storage.h"

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

struct Zarr : public Storage
{
  public:
    Zarr();
    explicit Zarr(BloscCompressionParams&& compression_params);
    virtual ~Zarr() noexcept = default;

    /// Storage interface
    virtual void set(const StorageProperties* props);
    virtual void get(StorageProperties* props) const;
    virtual void get_meta(StoragePropertyMetadata* meta) const;
    void start();
    int stop() noexcept;
    size_t append(const VideoFrame* frames, size_t nbytes);
    virtual void reserve_image_shape(const ImageShape* shape);

    /// Error state
    void set_error(const std::string& msg) noexcept;

  protected:
    using ChunkSize = StorageProperties::storage_properties_chunk_size_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    /// static - set on construction
    std::optional<BloscCompressionParams> blosc_compression_params_;

    /// changes on set
    fs::path dataset_root_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    uint32_t planes_per_chunk_;
    bool enable_multiscale_;

    /// changes on reserve_image_shape
    std::vector<std::pair<ImageDims, ImageDims>> image_tile_shapes_;
    std::vector<ImageShape> image_shapes_;
    std::vector<ChunkSize> chunk_sizes_;
    AppendDimension append_dimension_;
    SampleType pixel_type_;
    std::vector<std::shared_ptr<Writer>> writers_;

    /// changes on append
    // scaled frames, keyed by level-of-detail
    std::unordered_map<int, std::optional<VideoFrame*>> scaled_frames_;

    /// Multithreading
    std::shared_ptr<common::ThreadPool> thread_pool_;
    mutable std::mutex mutex_; // for error_ / error_msg_

    /// Error state
    bool error_;
    std::string error_msg_;

    /// Setup
    void set_chunking(const ChunkSize& size,
                      const ChunkingMeta& meta,
                      AppendDimension append_dimension);
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
};

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V0
