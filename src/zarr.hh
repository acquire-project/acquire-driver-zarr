#ifndef H_ACQUIRE_STORAGE_ZARR_V0
#define H_ACQUIRE_STORAGE_ZARR_V0

#include "device/kit/storage.h"
#include "platform.h"
#include "logger.h"

#include "prelude.h"
#include "chunk.writer.hh"

#include <string>
#include <optional>
#include <filesystem>
#include <queue>
#include <tuple>
#include <vector>

#ifndef __cplusplus
#error "This header requires C++20"
#endif

namespace acquire::sink::zarr {

struct Zarr;

struct ThreadContext
{
    Zarr* zarr;
    std::thread thread;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop;
};

struct ThreadJob
{
    std::shared_ptr<TiledFrame> frame;
    ChunkWriter* writer;
    std::function<bool(std::shared_ptr<TiledFrame>)> f;
};

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

/// \brief Zarr writer that conforms to v0.4 of the OME-NGFF specification.
///
/// This writes one multi-scale zarr image with one level/scale using the
/// OME-NGFF specification to determine the directory structure and contents
/// of group and array attributes.
///
/// https://ngff.openmicroscopy.org/0.4/
struct Zarr final : StorageInterface
{
    Zarr();
    explicit Zarr(BloscCompressor&& compressor);
    ~Zarr() override;

    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void get_meta(StoragePropertyMetadata* meta) const override;
    void start() override;
    [[nodiscard]] int stop() noexcept override;

    /// @return number of consumed bytes
    size_t append(const VideoFrame* frames, size_t nbytes) override;

    void reserve_image_shape(const ImageShape* shape) override;

    [[nodiscard]] bool pop_from_job_queue(ThreadJob& job);

  private:
    using ChunkingProps = StorageProperties::storage_properties_chunking_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    // static - set on construction
    char dimension_separator_;
    std::optional<BloscCompressor> compressor_;
    std::vector<ThreadContext> thread_pool_;

    // changes on set()
    std::string data_dir_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    size_t max_bytes_per_chunk_;
    ImageShape image_shape_;
    TileShape tile_shape_;
    std::vector<ChunkWriter*> writers_;
    size_t tiles_per_chunk_;

    // changes during acquisition
    size_t frame_count_;
    mutable std::mutex job_queue_mutex_;
    std::queue<ThreadJob> job_queue_;

    void set_chunking(const ChunkingProps& props, const ChunkingMeta& meta);

    void create_data_directory_() const;
    void write_zarray_json_() const;
    void write_external_metadata_json_() const;
    void write_zgroup_json_() const;
    void write_group_zattrs_json_() const;

    void allocate_writers_();
    void clear_writers_();

    void start_threads_();
    void recover_threads_();
};

// utilities

void
validate_props(const StorageProperties* props);

std::filesystem::path
as_path(const StorageProperties& props);

void
validate_image_shapes_equal(const ImageShape& lhs, const ImageShape& rhs);

const char*
sample_type_to_dtype(SampleType t);

const char*
sample_type_to_string(SampleType t) noexcept;

size_t
bytes_per_sample_type(SampleType t) noexcept;

void
validate_json(const char* str, size_t nbytes);

void
validate_directory_is_writable(const std::string& path);

size_t
get_bytes_per_frame(const ImageShape& image_shape) noexcept;

size_t
get_bytes_per_tile(const ImageShape& image_shape,
                   const TileShape& tile_shape) noexcept;

size_t
get_tiles_per_chunk(const ImageShape& image_shape,
                    const TileShape& tile_shape,
                    size_t max_bytes_per_chunk) noexcept;

size_t
get_bytes_per_chunk(const ImageShape& image_shape,
                    const TileShape& tile_shape,
                    size_t max_bytes_per_chunk) noexcept;

void
write_string(const std::string& path, const std::string& str);

void
worker_thread(ThreadContext* ctx);
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V0
