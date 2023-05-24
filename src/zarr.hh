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
#include <vector>

#ifndef __cplusplus
#error "This header requires C++20"
#endif

namespace acquire::sink::zarr {

using thread_t = thread;

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
template<Encoder E>
struct Zarr final : StorageInterface
{
    Zarr();
    explicit Zarr(size_t nthreads);
    ~Zarr() override;

    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void get_meta(StoragePropertyMetadata* meta) const override;
    void start() override;
    [[nodiscard]] int stop() noexcept override;

    /// @return number of consumed bytes
    size_t append(const VideoFrame* frames, size_t nbytes) override;

    void reserve_image_shape(const ImageShape* shape) override;

  private:
    using ChunkingProps = StorageProperties::storage_properties_chunking_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    // static - set on construction
    char dimension_separator_;

    // changes on set()
    std::queue<thread_t*> thread_pool_;
    std::string data_dir_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    size_t max_bytes_per_chunk_;
    ImageShape image_shape_;
    TileShape tile_shape_;
    std::vector<ChunkWriter*> writers_;
    std::optional<BloscCompressor> compressor_;
    size_t tiles_per_chunk_;

    // changes during acquisition
    size_t frame_count_;
    std::queue<TiledFrame*> frame_ptrs_;

    void set_chunking(const ChunkingProps& props, const ChunkingMeta& meta);

    void create_data_directory_() const;
    void write_zarray_json_() const;
    void write_external_metadata_json_() const;
    void write_zgroup_json_() const;
    void write_group_zattrs_json_() const;

    void initialize_thread_pool_(size_t nthreads);
    void finalize_thread_pool_();

    void allocate_writers_();
    void clear_writers_();

    void assign_threads_();
    void recover_threads_();

    void release_finished_frames_();
    size_t cycle_();
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

template<Encoder E>
Zarr<E>::Zarr()
  : dimension_separator_{ '/' }
  , frame_count_{ 0 }
  , pixel_scale_um_{ 1, 1 }
  , max_bytes_per_chunk_{ 0 }
  , tiles_per_chunk_{ 0 }
  , image_shape_{ 0 }
  , tile_shape_{ 0 }
{
    initialize_thread_pool_(1);
}

template<Encoder E>
Zarr<E>::Zarr(size_t nthreads)
  : dimension_separator_{ '/' }
  , frame_count_{ 0 }
  , pixel_scale_um_{ 1, 1 }
  , max_bytes_per_chunk_{ 0 }
  , tiles_per_chunk_{ 0 }
  , image_shape_{ 0 }
  , tile_shape_{ 0 }
{
    nthreads = std::clamp(
      nthreads, (size_t)1, (size_t)std::thread::hardware_concurrency());

    initialize_thread_pool_(nthreads);
}

template<Encoder E>
Zarr<E>::~Zarr()
{
    if (!stop())
        LOGE("Failed to stop on destruct!");
}

template<Encoder E>
void
Zarr<E>::set(const StorageProperties* props)
{
    using namespace acquire::sink::zarr;
    CHECK(props);

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    // checks the directory exists and is writable
    validate_props(props);
    data_dir_ = as_path(*props).string();

    if (props->external_metadata_json.str)
        external_metadata_json_ = props->external_metadata_json.str;

    pixel_scale_um_ = props->pixel_scale_um;

    // chunking
    set_chunking(props->chunking, meta.chunking);
}

template<Encoder E>
void
Zarr<E>::get(StorageProperties* props) const
{
    CHECK(Device_Ok == storage_properties_set_filename(
                         props, data_dir_.c_str(), data_dir_.size()));
    CHECK(Device_Ok == storage_properties_set_external_metadata(
                         props,
                         external_metadata_json_.c_str(),
                         external_metadata_json_.size()));
    props->pixel_scale_um = pixel_scale_um_;
}

template<Encoder E>
void
Zarr<E>::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    *meta = { .chunking = {
                .supported = 1,
                .max_bytes_per_chunk = { .writable = 1,
                                         .low = (float)(16 << 20),
                                         .high = (float)(1 << 31),
                                         .type = PropertyType_FixedPrecision },
              } };
}

template<Encoder E>
void
Zarr<E>::start()
{
    frame_count_ = 0;
    create_data_directory_();
    write_zgroup_json_();
    write_group_zattrs_json_();
    write_zarray_json_();
    write_external_metadata_json_();
}

template<Encoder E>
int
Zarr<E>::stop() noexcept
{
    int is_ok = 1;

    if (DeviceState_Running == state) {
        state = DeviceState_Armed;
        is_ok = 0;

        try {
            write_zarray_json_(); // must precede close of chunk file
            while (!frame_ptrs_.empty()) {
                TRACE("Cycling: %llu frames remaining", cycle_());
                clock_sleep_ms(nullptr, 50.0);
            }
            recover_threads_();
            finalize_thread_pool_();
            clear_writers_();
            is_ok = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
    }

    return is_ok;
}

template<Encoder E>
size_t
Zarr<E>::append(const VideoFrame* frames, size_t nbytes)
{
    if (0 == nbytes)
        return nbytes;

    using namespace acquire::sink::zarr;

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end; cur = next()) {
        frame_ptrs_.push(
          new TiledFrame(const_cast<VideoFrame*>(cur), tile_shape_));
        auto* tiled_frame = frame_ptrs_.back();

        // handle incoming image shape
        validate_image_shapes_equal(image_shape_, cur->shape);

        // push the new frame to our writers
        for (auto& writer : writers_)
            writer->push_frame(tiled_frame);

        ++frame_count_;
    }

    TRACE("Cycling: %lu frames on queue", cycle_());

    return nbytes;
}

template<Encoder E>
void
Zarr<E>::reserve_image_shape(const ImageShape* shape)
{
    CHECK(shape);
    image_shape_ = *shape;
    allocate_writers_();

    tiles_per_chunk_ =
      get_tiles_per_chunk(image_shape_, tile_shape_, max_bytes_per_chunk_);
}

template<Encoder E>
void
Zarr<E>::set_chunking(const ChunkingProps& props, const ChunkingMeta& meta)
{
    max_bytes_per_chunk_ = std::clamp(props.max_bytes_per_chunk,
                                      (uint32_t)meta.max_bytes_per_chunk.low,
                                      (uint32_t)meta.max_bytes_per_chunk.high);

    uint32_t tile_width = props.tile.width;
    if (tile_width == 0 ||
        (image_shape_.dims.width && tile_width > image_shape_.dims.width)) {
        LOGE("%s. Setting width to %u.",
             tile_width == 0 ? "Tile width not specified"
                             : "Specified roi width is too large",
             image_shape_.dims.width);
        tile_width = image_shape_.dims.width;
    }

    uint32_t tile_height = props.tile.height;
    if (tile_height == 0 ||
        (image_shape_.dims.height && tile_height > image_shape_.dims.height)) {
        LOGE("%s. Setting height to %u.",
             tile_height == 0 ? "Tile height not specified"
                              : "Specified roi height is too large",
             image_shape_.dims.height);
        tile_height = image_shape_.dims.height;
    }

    uint32_t tile_planes = props.tile.planes;
    if (tile_planes == 0 ||
        (image_shape_.dims.planes && tile_planes > image_shape_.dims.planes)) {
        LOGE("%s. Setting planes to %u.",
             tile_planes == 0 ? "Tile planes not specified"
                              : "Specified roi planes is too large",
             image_shape_.dims.planes);
        tile_planes = image_shape_.dims.planes;
    }

    tile_shape_ = {
        .dims = {
          .width = tile_width,
          .height = tile_height,
          .planes = tile_planes,
        },
        .frame_channels = {0}
    };
}

template<Encoder E>
void
Zarr<E>::initialize_thread_pool_(size_t nthreads)
{
    for (auto i = 0; i < nthreads; ++i) {
        auto t = new thread_t;
        thread_init(t);
        thread_pool_.push(t);
    }
}

template<Encoder E>
void
Zarr<E>::finalize_thread_pool_()
{
    while (!thread_pool_.empty()) {
        thread_t* t = thread_pool_.front();
        thread_pool_.pop();
        delete t;
    }
}

template<Encoder E>
void
Zarr<E>::create_data_directory_() const
{
    namespace fs = std::filesystem;
    if (fs::exists(data_dir_)) {
        std::error_code ec;
        EXPECT(fs::remove_all(data_dir_, ec),
               R"(Failed to remove folder for "%s": %s)",
               data_dir_.c_str(),
               ec.message().c_str());
    }

    EXPECT(fs::create_directory(data_dir_),
           "Failed to create folder for \"%s\"",
           data_dir_.c_str());
}

template<Encoder E>
void
Zarr<E>::write_zarray_json_() const
{
    namespace fs = std::filesystem;
    using namespace acquire::sink::zarr;
    using json = nlohmann::json;

    const auto frames_per_chunk = std::min(frame_count_, tiles_per_chunk_);

    json zarray_attrs = {
        { "zarr_format", 2 },
        { "shape",
          {
            (uint64_t)frame_count_,
            image_shape_.dims.channels,
            image_shape_.dims.height,
            image_shape_.dims.width,
          } },
        { "chunks",
          {
            (uint64_t)frames_per_chunk,
            1,
            tile_shape_.dims.height,
            tile_shape_.dims.width,
          } },
        { "dtype", sample_type_to_dtype(image_shape_.type) },
        { "fill_value", 0 },
        { "order", "C" },
        { "filters", nullptr },
        { "dimension_separator", std::string(1, dimension_separator_) },
    };

    if (compressor_.has_value())
        zarray_attrs["compressor"] = compressor_.value();
    else
        zarray_attrs["compressor"] = nullptr;

    std::string zarray_path = (fs::path(data_dir_) / "0" / ".zarray").string();
    write_string(zarray_path, zarray_attrs.dump());
}

template<Encoder E>
void
Zarr<E>::write_external_metadata_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (fs::path(data_dir_) / "0" / ".zattrs").string();
    write_string(zattrs_path, external_metadata_json_);
}

template<Encoder E>
void
Zarr<E>::write_group_zattrs_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    json zgroup_attrs;
    zgroup_attrs["multiscales"] = json::array({ json::object() });
    zgroup_attrs["multiscales"][0]["version"] = "0.4";
    zgroup_attrs["multiscales"][0]["axes"] = {
        {
          { "name", "t" },
          { "type", "time" },
        },
        {
          { "name", "c" },
          { "type", "channel" },
        },
        {
          { "name", "y" },
          { "type", "space" },
          { "unit", "micrometer" },
        },
        {
          { "name", "x" },
          { "type", "space" },
          { "unit", "micrometer" },
        },
    };
    zgroup_attrs["multiscales"][0]["datasets"] = {
        {
          { "path", "0" },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", { 1, 1, pixel_scale_um_.y, pixel_scale_um_.x } },
              },
            } },
        },
    };

    std::string zattrs_path = (fs::path(data_dir_) / ".zattrs").string();
    write_string(zattrs_path, zgroup_attrs.dump(4));
}

template<Encoder E>
void
Zarr<E>::write_zgroup_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (fs::path(data_dir_) / ".zgroup").string();
    write_string(zgroup_path, zgroup.dump());
}

template<Encoder E>
void
Zarr<E>::allocate_writers_()
{
    auto frame_rois = make_frame_rois(image_shape_, tile_shape_);
    CHECK(!frame_rois.empty());
    TRACE("Allocating %llu writers", frame_rois.size());

    std::for_each(
      writers_.begin(), writers_.end(), [](ChunkWriter* w) { delete w; });
    writers_.clear();
    for (const auto& roi : frame_rois) {
        E* encoder;
        CHECK(encoder = new E());
        if (encoder->get_compressor()) {
            size_t bytes_per_chunk = get_bytes_per_chunk(
              image_shape_, tile_shape_, max_bytes_per_chunk_);
            encoder->allocate_buffer(bytes_per_chunk);
        } else {
            size_t bytes_per_tile =
              get_bytes_per_tile(image_shape_, tile_shape_);
            encoder->allocate_buffer(bytes_per_tile);
        }

        encoder->set_bytes_per_pixel(bytes_per_sample_type(image_shape_.type));
        auto writer = new ChunkWriter(roi, max_bytes_per_chunk_, encoder);
        CHECK(writer);

        writer->set_dimension_separator(dimension_separator_);
        writer->set_base_directory(data_dir_);
        writers_.push_back(writer);
    }

    while (writers_.size() < thread_pool_.size()) {
        thread_t* t = thread_pool_.front();
        thread_pool_.pop();
        delete t;
    }
}

template<Encoder E>
void
Zarr<E>::clear_writers_()
{
    for (auto& writer : writers_) {
        writer->close_current_file();
        delete writer;
    }
    writers_.clear();
}

template<Encoder E>
void
Zarr<E>::assign_threads_()
{
    // sort threads by number of frames still needing to be written
    std::sort(
      writers_.begin(), writers_.end(), [](ChunkWriter* a, ChunkWriter* b) {
          return a->active_frames() > b->active_frames();
      });

    // thread_pool_ has at most as many threads as there are writers
    while (!thread_pool_.empty()) {
        thread_t* t = thread_pool_.front();
        thread_pool_.pop();

        while (t != nullptr) {
            for (auto& writer : writers_) {
                if (!writer->has_thread()) {
                    writer->assign_thread(&t);
                    break;
                }
            }
        }
    }
}

template<Encoder E>
void
Zarr<E>::recover_threads_()
{
    for (auto& writer : writers_) {
        thread_t* t = writer->release_thread();
        if (nullptr != t)
            thread_pool_.push(t);
    }
}

/// make a single pass through the frame queue and check if any writers are
/// still using any queued frames
template<Encoder E>
void
Zarr<E>::release_finished_frames_()
{
    const size_t nframes = frame_ptrs_.size();
    for (auto i = 0; i < nframes; ++i) {
        TiledFrame* frame = frame_ptrs_.front();
        frame_ptrs_.pop();

        bool replace_frame = false;
        for (auto& writer : writers_) {
            if (writer->has_frame(frame->frame_id())) {
                replace_frame = true;
                break;
            }
        }

        if (replace_frame)
            frame_ptrs_.push(frame);
        else
            delete frame;
    }
}

template<Encoder E>
size_t
Zarr<E>::cycle_()
{
    if (writers_.size() > thread_pool_.size())
        recover_threads_();

    release_finished_frames_();

    if (!frame_ptrs_.empty())
        assign_threads_();

    return frame_ptrs_.size();
}

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V0
