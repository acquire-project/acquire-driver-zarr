#include "zarr.hh"

#include "device/kit/storage.h"
#include "logger.h"
#include "platform.h"
#include "zarr.raw.hh"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>

#include "json.hpp"

namespace fs = std::filesystem;
namespace zarr = acquire::sink::zarr;

//
// Private namespace
//

namespace {

// Forward declarations

DeviceState
zarr_set(Storage*, const StorageProperties* props) noexcept;

void
zarr_get(const Storage*, StorageProperties* props) noexcept;

void
zarr_get_meta(const Storage*, StoragePropertyMetadata* meta) noexcept;

DeviceState
zarr_start(Storage*) noexcept;

DeviceState
zarr_append(Storage* self_, const VideoFrame* frame, size_t* nbytes) noexcept;

DeviceState
zarr_stop(Storage*) noexcept;

void
zarr_destroy(Storage*) noexcept;

//
// STORAGE C API IMPLEMENTATIONS
//

DeviceState
zarr_set(Storage* self_, const StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        self->set(props);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
        return DeviceState_AwaitingConfiguration;
    } catch (...) {
        LOGE("Exception: (unknown)");
        return DeviceState_AwaitingConfiguration;
    }

    return DeviceState_Armed;
}

void
zarr_get(const Storage* self_, StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        self->get(props);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

void
zarr_get_meta(const Storage* self_, StoragePropertyMetadata* meta) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        self->get_meta(meta);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

DeviceState
zarr_start(Storage* self_) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };

    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        self->start();
        state = DeviceState_Running;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return state;
}

DeviceState
zarr_append(Storage* self_, const VideoFrame* frames, size_t* nbytes) noexcept
{
    DeviceState state;
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        *nbytes = self->append(frames, *nbytes);
        state = DeviceState_Running;
    } catch (const std::exception& exc) {
        *nbytes = 0;
        LOGE("Exception: %s\n", exc.what());
        state = DeviceState_AwaitingConfiguration;
    } catch (...) {
        *nbytes = 0;
        LOGE("Exception: (unknown)");
        state = DeviceState_AwaitingConfiguration;
    }

    return state;
}

DeviceState
zarr_stop(Storage* self_) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };

    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        CHECK(self->stop());
        state = DeviceState_Armed;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return state;
}

void
zarr_destroy(Storage* self_) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        if (self_->stop)
            self_->stop(self_);

        delete self;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

void
zarr_reserve_image_shape(Storage* self_, const ImageShape* shape) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::StorageInterface*)self_;
        self->reserve_image_shape(shape);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}
} // end namespace ::{anonymous}

//
// zarr namespace implementations
//

zarr::Zarr::Zarr()
  : dimension_separator_{ '/' }
  , frame_count_{ 0 }
  , pixel_scale_um_{ 1, 1 }
  , max_bytes_per_chunk_{ 0 }
  , image_shape_{ 0 }
  , tile_shape_{ 0 }
  , thread_pool_(std::thread::hardware_concurrency())
{
    start_threads_();
}

zarr::Zarr::Zarr(CompressionParams&& compression_params)
  : dimension_separator_{ '/' }
  , frame_count_{ 0 }
  , pixel_scale_um_{ 1, 1 }
  , max_bytes_per_chunk_{ 0 }
  , image_shape_{ 0 }
  , tile_shape_{ 0 }
  , thread_pool_(std::thread::hardware_concurrency())
{
    compression_params_ = std::move(compression_params);
    start_threads_();
}

zarr::Zarr::~Zarr()
{
    if (!stop()) {
        LOGE("Failed to stop on destruct!");
    }
    recover_threads_();
}

void
zarr::Zarr::set(const StorageProperties* props)
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

    // hang on to this until we have the image shape
    enable_multiscale_ = (bool)props->enable_multiscale;
}

void
zarr::Zarr::get(StorageProperties* props) const
{
    CHECK(storage_properties_set_filename(
      props, data_dir_.c_str(), data_dir_.size()));
    CHECK(storage_properties_set_external_metadata(
      props, external_metadata_json_.c_str(), external_metadata_json_.size()));
    props->pixel_scale_um = pixel_scale_um_;

    props->chunking.tile.width = tile_shape_.width;
    props->chunking.tile.height = tile_shape_.height;
    props->chunking.tile.planes = tile_shape_.planes;

    props->enable_multiscale = enable_multiscale_;
}

void
zarr::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    *meta = {
        .chunking = {
          .supported = 1,
          .max_bytes_per_chunk = {
            .writable = 1,
            .low = (float)(16 << 20),
            .high = (float)(1 << 30),
            .type = PropertyType_FixedPrecision },
        },
        .multiscale = {
          .supported = 1,
        }
    };
}

void
zarr::Zarr::start()
{
    frame_count_ = 0;
    create_data_directory_();
    write_zgroup_json_();
    write_group_zattrs_json_();
    write_zarray_json_();
    write_external_metadata_json_();
}

int
zarr::Zarr::stop() noexcept
{
    int is_ok = 1;

    if (DeviceState_Running == state) {
        state = DeviceState_Armed;
        is_ok = 0;

        try {
            while (!job_queue_.empty()) {
                TRACE("Cycling: %llu jobs remaining", job_queue_.size());
                clock_sleep_ms(nullptr, 50.0);
            }
            recover_threads_();
            write_zarray_json_();       // must precede close of chunk file
            write_group_zattrs_json_(); // write multiscales metadata
            is_ok = 1;
            frame_count_ = 0;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
    }

    return is_ok;
}

size_t
zarr::Zarr::append(const VideoFrame* frames, size_t nbytes)
{
    if (0 == nbytes)
        return nbytes;

    // validate start conditions
    if (0 == frame_count_) {
        validate_image_and_tile_shapes_();
    }

    using namespace acquire::sink::zarr;

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end;) {
        // check that the number of jobs on the queue is not too large
        {
            std::scoped_lock lock(job_queue_mutex_);
            if (job_queue_.size() >= queue_cap_) {
                TRACE("Queue size at or beyond capacity: %llu > %llu",
                      job_queue_.size(),
                      queue_cap_);

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
        }

        // handle incoming image shape
        validate_image_shapes_equal(image_shape_, cur->shape);

        // create a new frame
        auto frame =
          std::make_shared<TiledFrame>(cur, image_shape_, tile_shape_);

        if (frame_scaler_) {
            std::scoped_lock lock(job_queue_mutex_);

            // push the new frame to our scaler
            job_queue_.emplace(
              [this, frame]() { return frame_scaler_->push_frame(frame); });
        } else {
            push_frame_to_writers(frame);
        }

        ++frame_count_;

        cur = next();
    }

    return nbytes;
}

void
zarr::Zarr::reserve_image_shape(const ImageShape* shape)
{
    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);
    image_shape_ = *shape;

    // ensure that tile dimensions are compatible with the image shape
    {
        StorageProperties props = { 0 };
        get(&props);

        uint32_t tile_width = props.chunking.tile.width;
        if (image_shape_.dims.width > 0 &&
            (tile_width == 0 || tile_width > image_shape_.dims.width)) {
            LOGE("%s. Setting width to %u.",
                 tile_width == 0 ? "Tile width not specified"
                                 : "Specified tile width is too large",
                 image_shape_.dims.width);
            tile_width = image_shape_.dims.width;
        }
        tile_shape_.width = tile_width;

        uint32_t tile_height = props.chunking.tile.height;
        if (image_shape_.dims.height > 0 &&
            (tile_height == 0 || tile_height > image_shape_.dims.height)) {
            LOGE("%s. Setting height to %u.",
                 tile_height == 0 ? "Tile height not specified"
                                  : "Specified tile height is too large",
                 image_shape_.dims.height);
            tile_height = image_shape_.dims.height;
        }
        tile_shape_.height = tile_height;

        uint32_t tile_planes = props.chunking.tile.planes;
        if (image_shape_.dims.planes > 0 &&
            (tile_planes == 0 || tile_planes > image_shape_.dims.planes)) {
            LOGE("%s. Setting planes to %u.",
                 tile_planes == 0 ? "Tile plane count not specified"
                                  : "Specified tile plane count is too large",
                 image_shape_.dims.planes);
            tile_planes = image_shape_.dims.planes;
        }
        tile_shape_.planes = tile_planes;
        storage_properties_destroy(&props);
    }

    // ensure that the chunk size can accommodate at least one tile
    uint64_t bytes_per_tile = get_bytes_per_tile(image_shape_, tile_shape_);
    if (max_bytes_per_chunk_ < bytes_per_tile) {
        LOGE("Specified chunk size %llu is too small. Setting to %llu bytes.",
             max_bytes_per_chunk_,
             bytes_per_tile);
        max_bytes_per_chunk_ = bytes_per_tile;
    }

    if (enable_multiscale_) {
        frame_scaler_.emplace(this, image_shape_, tile_shape_);
    }

    allocate_writers_();

    // ensure the data stored on the queue takes up no more than 1 GiB
    queue_cap_ = std::max((uint64_t)1, (uint64_t)(1 << 30) / bytes_per_tile);
}

void
zarr::Zarr::push_frame_to_writers(const std::shared_ptr<TiledFrame> frame)
{
    std::scoped_lock lock(job_queue_mutex_);
    auto writer = writers_.at(frame->layer());

    for (auto& w : writer) {
        job_queue_.emplace([w, frame]() { return w->write_frame(*frame); });
    }
}

std::optional<zarr::Zarr::JobT>
zarr::Zarr::pop_from_job_queue()
{
    std::scoped_lock lock(job_queue_mutex_);
    if (job_queue_.empty())
        return {};

    auto job = job_queue_.front();
    job_queue_.pop();

    return { job };
}

void
zarr::Zarr::set_chunking(const ChunkingProps& props, const ChunkingMeta& meta)
{
    max_bytes_per_chunk_ = std::clamp(props.max_bytes_per_chunk,
                                      (uint64_t)meta.max_bytes_per_chunk.low,
                                      (uint64_t)meta.max_bytes_per_chunk.high);

    tile_shape_ = {
        .width = props.tile.width,
        .height = props.tile.height,
        .planes = props.tile.planes,
    };
}

void
zarr::Zarr::create_data_directory_() const
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

void
zarr::Zarr::write_zarray_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (writers_.empty()) {
        write_zarray_json_inner_(0, image_shape_, tile_shape_);
    } else {
        for (const auto& [layer, writers] : writers_) {
            auto writer = writers.front();
            const auto& is = writer->image_shape();
            const auto& ts = writer->tile_shape();
            write_zarray_json_inner_(layer, is, ts);
        }
    }
}

void
zarr::Zarr::write_zarray_json_inner_(size_t layer,
                                     const ImageShape& image_shape,
                                     const TileShape& tile_shape) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (!writers_.contains(layer)) {
        return;
    }

    const uint64_t frame_count = writers_.at(layer).front()->frames_written();
    const auto frames_per_chunk =
      std::min(frame_count,
               (uint64_t)get_tiles_per_chunk(
                 image_shape, tile_shape, max_bytes_per_chunk_));

    json zarray_attrs = {
        { "zarr_format", 2 },
        { "shape",
          {
            frame_count,
            image_shape.dims.channels,
            image_shape.dims.height,
            image_shape.dims.width,
          } },
        { "chunks",
          {
            frames_per_chunk,
            1,
            tile_shape.height,
            tile_shape.width,
          } },
        { "dtype", sample_type_to_dtype(image_shape.type) },
        { "fill_value", 0 },
        { "order", "C" },
        { "filters", nullptr },
        { "dimension_separator", std::string(1, dimension_separator_) },
    };

    if (compression_params_.has_value())
        zarray_attrs["compressor"] = compression_params_.value();
    else
        zarray_attrs["compressor"] = nullptr;

    std::string zarray_path =
      (fs::path(data_dir_) / std::to_string(layer) / ".zarray").string();
    write_string(zarray_path, zarray_attrs.dump());
}

void
zarr::Zarr::write_external_metadata_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (fs::path(data_dir_) / "0" / ".zattrs").string();
    write_string(zattrs_path, external_metadata_json_);
}

void
zarr::Zarr::write_group_zattrs_json_() const
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

    // spatial multiscale metadata
    if (writers_.empty() || !frame_scaler_.has_value()) {
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
    } else {
        for (const auto& [layer, _] : writers_) {
            zgroup_attrs["multiscales"][0]["datasets"].push_back({
              { "path", std::to_string(layer) },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale",
                      { std::pow(2, layer),
                        1,
                        std::pow(2, layer) * pixel_scale_um_.y,
                        std::pow(2, layer) * pixel_scale_um_.x } },
                  },
                } },
            });
        }

        // downsampling metadata
        zgroup_attrs["multiscales"][0]["type"] = "local_mean";
        zgroup_attrs["multiscales"][0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters are "
              "given here." },
            { "method", "skimage.transform.downscale_local_mean" },
            { "version", "0.21.0" },
            { "args", "[2]" },
            { "kwargs", { "cval", 0 } },
        };
    }

    std::string zattrs_path = (fs::path(data_dir_) / ".zattrs").string();
    write_string(zattrs_path, zgroup_attrs.dump(4));
}

void
zarr::Zarr::write_zgroup_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (fs::path(data_dir_) / ".zgroup").string();
    write_string(zgroup_path, zgroup.dump());
}

void
zarr::Zarr::allocate_writers_()
{
    writers_.clear();

    std::vector<ScalingParameters> scaling_params;
    if (frame_scaler_) {
        scaling_params = make_scaling_parameters(image_shape_, tile_shape_);
    } else {
        scaling_params.emplace_back(image_shape_, tile_shape_);
    }

    for (auto layer = 0; layer < scaling_params.size(); ++layer) {
        auto multiscale = scaling_params.at(layer);
        auto& image_shape = multiscale.image_shape;
        auto& tile_shape = multiscale.tile_shape;

        CHECK(tile_shape.width > 0);
        size_t img_px_x = image_shape.dims.channels * image_shape.dims.width;
        size_t tile_cols = std::ceil((float)img_px_x / (float)tile_shape.width);

        size_t img_px_y = image_shape_.dims.height;
        CHECK(tile_shape_.height > 0);
        size_t tile_rows =
          std::ceil((float)img_px_y / (float)tile_shape_.height);

        size_t img_px_p = image_shape_.dims.planes;
        CHECK(tile_shape_.planes > 0);
        size_t tile_planes =
          std::ceil((float)img_px_p / (float)tile_shape_.planes);

        TRACE("Allocating %llu writers for layer %d",
              tile_cols * tile_rows * tile_planes,
              layer);

        size_t buf_size =
          compression_params_.has_value()
            ? get_bytes_per_chunk(image_shape, tile_shape, max_bytes_per_chunk_)
            : get_bytes_per_tile(image_shape, tile_shape);

        writers_.emplace(layer, std::vector<std::shared_ptr<ChunkWriter>>());
        for (auto plane = 0; plane < tile_planes; ++plane) {
            for (auto row = 0; row < tile_rows; ++row) {
                for (auto col = 0; col < tile_cols; ++col) {
                    BaseEncoder* encoder;
                    if (compression_params_.has_value()) {
                        CHECK(encoder =
                                new BloscEncoder(compression_params_.value()));
                    } else {
                        CHECK(encoder = new RawEncoder());
                    }

                    encoder->allocate_buffer(buf_size);
                    encoder->set_bytes_per_pixel(
                      bytes_per_sample_type(image_shape_.type));
                    writers_.at(layer).push_back(
                      std::make_shared<ChunkWriter>(encoder,
                                                    image_shape,
                                                    tile_shape,
                                                    layer,
                                                    col,
                                                    row,
                                                    plane,
                                                    max_bytes_per_chunk_,
                                                    dimension_separator_,
                                                    data_dir_));
                }
            }
        }
    }
}

void
zarr::Zarr::validate_image_and_tile_shapes_() const
{
    CHECK(image_shape_.dims.channels > 0);
    CHECK(image_shape_.dims.width > 0);
    CHECK(image_shape_.dims.height > 0);
    CHECK(image_shape_.dims.planes > 0);
    CHECK(tile_shape_.width > 0);
    CHECK(tile_shape_.width <= image_shape_.dims.width);
    CHECK(tile_shape_.height > 0);
    CHECK(tile_shape_.height <= image_shape_.dims.height);
    CHECK(tile_shape_.planes > 0);
    CHECK(tile_shape_.planes <= image_shape_.dims.planes);
}

void
zarr::Zarr::start_threads_()
{
    for (auto& ctx : thread_pool_) {
        std::scoped_lock lock(ctx.mutex);
        ctx.zarr = this;
        ctx.should_stop = false;
        ctx.thread = std::thread(worker_thread, &ctx);
        ctx.cv.notify_one();
    }
}

void
zarr::Zarr::recover_threads_()
{
    for (auto& ctx : thread_pool_) {
        {
            std::scoped_lock lock(ctx.mutex);
            ctx.should_stop = true;
            ctx.cv.notify_one();
        }

        if (ctx.thread.joinable()) {
            ctx.thread.join();
        }
    }
}

/// \brief Check that the StorageProperties are valid.
/// \details Assumes either an empty or valid JSON metadata string and a
/// filename string that points to a writable directory. \param props Storage
/// properties for Zarr. \throw std::runtime_error if the parent of the Zarr
/// data directory is not an existing directory.
void
zarr::validate_props(const StorageProperties* props)
{
    EXPECT(props->filename.str, "Filename string is NULL.");
    EXPECT(props->filename.nbytes, "Filename string is zero size.");

    // check that JSON is correct (throw std::exception if not)
    validate_json(props->external_metadata_json.str,
                  props->external_metadata_json.nbytes);

    // check that the filename value points to a writable directory
    {

        auto path = as_path(*props);
        auto parent_path = path.parent_path().string();
        if (parent_path.empty())
            parent_path = ".";

        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.c_str());
        validate_directory_is_writable(parent_path);
    }
}

/// \brief Get the filename from a StorageProperties as fs::path.
/// \param props StorageProperties for the Zarr Storage device.
/// \return fs::path representation of the Zarr data directory.
fs::path
zarr::as_path(const StorageProperties& props)
{
    return { props.filename.str,
             props.filename.str + props.filename.nbytes - 1 };
}

/// \brief Check that two ImageShapes are equivalent, i.e., that the data types
/// agree and the dimensions are equal.
/// \param lhs An ImageShape.
/// \param rhs Another ImageShape.
/// \throw std::runtime_error if the ImageShapes have different data types or
/// dimensions.
void
zarr::validate_image_shapes_equal(const ImageShape& lhs, const ImageShape& rhs)
{
    EXPECT(lhs.type == rhs.type,
           "Datatype mismatch! Expected: %s. Got: %s.",
           sample_type_to_string(lhs.type),
           sample_type_to_string(rhs.type));

    EXPECT(lhs.dims.channels == rhs.dims.channels &&
             lhs.dims.width == rhs.dims.width &&
             lhs.dims.height == rhs.dims.height,
           "Dimension mismatch! Expected: (%d, %d, %d). Got (%d, %d, "
           "%d)",
           lhs.dims.channels,
           lhs.dims.width,
           lhs.dims.height,
           rhs.dims.channels,
           rhs.dims.width,
           rhs.dims.height);
}

/// \brief Get the Zarr dtype for a given SampleType.
/// \param t An enumerated sample type.
/// \throw std::runtime_error if \par t is not a valid SampleType.
/// \return A representation of the SampleType \par t expected by a Zarr reader.
const char*
zarr::sample_type_to_dtype(SampleType t)
{
    static const char* table[] = { "<u1", "<u2", "<i1", "<i2",
                                   "<f4", "<u2", "<u2", "<u2" };
    if (t < countof(table)) {
        return table[t];
    } else {
        throw std::runtime_error("Invalid sample type.");
    }
}

/// \brief Get a string representation of the SampleType enum.
/// \param t An enumerated sample type.
/// \return A human-readable representation of the SampleType \par t.
const char*
zarr::sample_type_to_string(SampleType t) noexcept
{
    static const char* table[] = { "u8",  "u16", "i8",  "i16",
                                   "f32", "u16", "u16", "u16" };
    if (t < countof(table)) {
        return table[t];
    } else {
        return "unrecognized pixel type";
    }
}

/// \brief Get the number of bytes for a given SampleType.
/// \param t An enumerated sample type.
/// \return The number of bytes the SampleType \par t represents.
size_t
zarr::bytes_per_sample_type(SampleType t) noexcept
{
    static size_t table[] = { 1, 2, 1, 2, 4, 2, 2, 2 };
    if (t < countof(table)) {
        return table[t];
    } else {
        LOGE("Invalid sample type.");
        return 0;
    }
}

/// \brief Check that the JSON string is valid. (Valid can mean empty.)
/// \param str Putative JSON metadata string.
/// \param nbytes Size of the JSON metadata char array
void
zarr::validate_json(const char* str, size_t nbytes)
{
    // Empty strings are valid (no metadata is fine).
    if (nbytes <= 1 || nullptr == str) {
        return;
    }

    // Don't do full json validation here, but make sure it at least
    // begins and ends with '{' and '}'
    EXPECT(nbytes >= 3,
           "nbytes (%d) is too small. Expected a null-terminated json string.",
           (int)nbytes);
    EXPECT(str[nbytes - 1] == '\0', "String must be null-terminated");
    EXPECT(str[0] == '{', "json string must start with \'{\'");
    EXPECT(str[nbytes - 2] == '}', "json string must end with \'}\'");
}

/// \brief Check that the argument is a writable directory.
/// \param path The path to check.
/// \throw std::runtime_error if \par path is either not a directory or not
/// writable.
void
zarr::validate_directory_is_writable(const std::string& path)
{
    EXPECT(fs::is_directory(path),
           "Expected \"%s\" to be a directory.",
           path.c_str());

    const auto perms = fs::status(fs::path(path)).permissions();

    EXPECT((perms & (fs::perms::owner_write | fs::perms::group_write |
                     fs::perms::others_write)) != fs::perms::none,
           "Expected \"%s\" to have write permissions.",
           path.c_str());
}

/// \brief Compute the number of bytes in a frame, given an image shape.
/// \param image_shape Description of the image's shape.
/// \return The number of bytes to expect in a frame.
size_t
zarr::get_bytes_per_frame(const ImageShape& image_shape) noexcept
{
    return zarr::bytes_per_sample_type(image_shape.type) *
           image_shape.dims.channels * image_shape.dims.height *
           image_shape.dims.width * image_shape.dims.planes;
}

/// \brief Compute the number of bytes in a tile, given an image shape and a
///        tile shape.
/// \param image_shape Description of the image's shape.
/// \param tile_shape Description of the tile's shape.
/// \return The number of bytes to expect in a tile.
size_t
zarr::get_bytes_per_tile(const ImageShape& image_shape,
                         const TileShape& tile_shape) noexcept
{
    return zarr::bytes_per_sample_type(image_shape.type) *
           image_shape.dims.channels * tile_shape.height * tile_shape.width *
           tile_shape.planes;
}

uint32_t
zarr::get_tiles_per_chunk(const ImageShape& image_shape,
                          const TileShape& tile_shape,
                          uint64_t max_bytes_per_chunk) noexcept
{
    auto bpt = (float)get_bytes_per_tile(image_shape, tile_shape);
    if (0 == bpt)
        return 0;
    return (uint32_t)std::floor((float)max_bytes_per_chunk / bpt);
}

size_t
zarr::get_bytes_per_chunk(const ImageShape& image_shape,
                          const TileShape& tile_shape,
                          size_t max_bytes_per_chunk) noexcept
{
    return get_bytes_per_tile(image_shape, tile_shape) *
           get_tiles_per_chunk(image_shape, tile_shape, max_bytes_per_chunk);
}

/// \brief Write a string to a file.
/// @param path The path of the file to write.
/// @param str The string to write.
void
zarr::write_string(const std::string& path, const std::string& str)
{
    if (auto p = fs::path(path); !fs::exists(p.parent_path()))
        fs::create_directories(p.parent_path());

    struct file f = { 0 };
    auto is_ok = file_create(&f, path.c_str(), path.size());
    is_ok &= file_write(&f,                                  // file
                        0,                                   // offset
                        (uint8_t*)str.c_str(),               // cur
                        (uint8_t*)(str.c_str() + str.size()) // end
    );
    EXPECT(is_ok, "Write to \"%s\" failed.", path.c_str());
    TRACE("Wrote %d bytes to \"%s\".", str.size(), path.c_str());
    file_close(&f);
}

void
zarr::worker_thread(ThreadContext* ctx)
{
    using namespace std::chrono_literals;

    TRACE("Worker thread starting.");
    CHECK(ctx);

    while (true) {
        std::unique_lock lock(ctx->mutex);
        ctx->cv.wait_for(lock, 5ms, [&] { return ctx->should_stop; });

        if (ctx->should_stop) {
            break;
        }

        if (auto job = ctx->zarr->pop_from_job_queue(); job.has_value()) {
            CHECK(job.value()());
        }
    }

    TRACE("Worker thread exiting.");
}

zarr::StorageInterface::StorageInterface()
  : Storage{
      .state = DeviceState_AwaitingConfiguration,
      .set = ::zarr_set,
      .get = ::zarr_get,
      .get_meta = ::zarr_get_meta,
      .start = ::zarr_start,
      .append = ::zarr_append,
      .stop = ::zarr_stop,
      .destroy = ::zarr_destroy,
      .reserve_image_shape = ::zarr_reserve_image_shape,
  }
{
}

extern "C" struct Storage*
zarr_init()
{
    try {
        return new zarr::Zarr();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
