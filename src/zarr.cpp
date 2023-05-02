#include "zarr.hh"

#include "device/kit/storage.h"
#include "logger.h"
#include "platform.h"

#include <algorithm>
#include <cmath>
#include <thread>

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
} // end namespace ::{anonymous}

//
// zarr namespace implementations
//
zarr::Zarr::Zarr()
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

zarr::Zarr::Zarr(size_t nthreads)
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

zarr::Zarr::~Zarr()
{
    if (!stop())
        LOGE("Failed to stop on destruct!");
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

    image_shape_ = props->image_shape;

    // chunking
    set_chunking(props->chunking, meta.chunking);

    // compression
    set_compressor(props->compression, meta.compression);
}

void
zarr::Zarr::get(StorageProperties* props) const
{
    CHECK(Device_Ok == storage_properties_set_filename(
                         props, data_dir_.c_str(), data_dir_.size()));
    CHECK(Device_Ok == storage_properties_set_external_metadata(
                         props,
                         external_metadata_json_.c_str(),
                         external_metadata_json_.size()));
    props->pixel_scale_um = pixel_scale_um_;
}

void
zarr::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    *meta = {
        .file_control = {
          .supported = 1,
          .default_extension = { 0 },
        },
        .chunking = {
          .supported = 1,
          .bytes_per_chunk = {
            .writable = 1,
            .low = (float)(16 << 20),
            .high = (float)(1 << 31),
            .type = PropertyType_FixedPrecision
          },
        },
        .compression = {
          .supported = 1,
          .clevel = {
            .writable = 1,
            .low = 1,
            .high = 9,
            .type = PropertyType_FixedPrecision
          },
          .shuffle = {
            .writable = 1,
            .low = 0,
            .high = 2,
            .type = PropertyType_FixedPrecision
          },
        }
    };
    strncpy(meta->file_control.default_extension, ".zarr", sizeof(".zarr"));
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
    allocate_writers_();
}

int
zarr::Zarr::stop() noexcept
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

size_t
zarr::Zarr::append(const VideoFrame* frames, size_t nbytes)
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

void
zarr::Zarr::set_chunking(const ChunkingProps& props, const ChunkingMeta& meta)
{
    max_bytes_per_chunk_ = std::clamp(props.bytes_per_chunk,
                                      (uint32_t)meta.bytes_per_chunk.low,
                                      (uint32_t)meta.bytes_per_chunk.high);

    uint32_t tile_width = props.tile_width;
    if (tile_width == 0 ||
        (image_shape_.dims.width && tile_width > image_shape_.dims.width)) {
        LOGE("%s. Setting width to %u.",
             tile_width == 0 ? "Tile width not specified"
                             : "Specified roi width is too large",
             image_shape_.dims.width);
        tile_width = image_shape_.dims.width;
    }

    uint32_t tile_height = props.tile_height;
    if (tile_height == 0 ||
        (image_shape_.dims.height && tile_height > image_shape_.dims.height)) {
        LOGE("%s. Setting height to %u.",
             tile_height == 0 ? "Tile height not specified"
                              : "Specified roi height is too large",
             image_shape_.dims.height);
        tile_height = image_shape_.dims.height;
    }

    uint32_t tile_planes = props.tile_planes;
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

    tiles_per_chunk_ =
      get_tiles_per_chunk(image_shape_, tile_shape_, max_bytes_per_chunk_);
}

void
zarr::Zarr::set_compressor(const CompressionProps& props,
                           const CompressionMeta& meta)
{
    if (props.codec_id.nbytes < 2 || nullptr == props.codec_id.str)
        return;

    auto supported_codecs = BloscCompressor::supported_codecs();

    EXPECT(std::find(supported_codecs.begin(),
                     supported_codecs.end(),
                     props.codec_id.str) != supported_codecs.end(),
           R"(Unsupported value "%s" for compression codec.)",
           props.codec_id.str);

    EXPECT(props.clevel >= meta.clevel.low && props.clevel <= meta.clevel.high,
           "Unsupported value %llu for clevel. Expected a value between "
           "%lu and %lu.",
           props.clevel,
           (size_t)meta.clevel.low,
           (size_t)meta.clevel.high);

    EXPECT(props.shuffle >= meta.shuffle.low &&
             props.shuffle <= meta.shuffle.high,
           "Unsupported value %llu for shuffle. Expected a value between "
           "%lu and %lu.",
           props.shuffle,
           (size_t)meta.shuffle.low,
           (size_t)meta.shuffle.high);

    compressor_ = BloscCompressor(
      std::string(props.codec_id.str), props.clevel, props.shuffle);
}

void
zarr::Zarr::initialize_thread_pool_(size_t nthreads)
{
    for (auto i = 0; i < nthreads; ++i) {
        auto t = new thread_t;
        thread_init(t);
        thread_pool_.push(t);
    }
}

void
zarr::Zarr::finalize_thread_pool_()
{
    while (!thread_pool_.empty()) {
        thread_t* t = thread_pool_.front();
        thread_pool_.pop();
        delete t;
    }
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
    auto frame_rois = make_frame_rois(image_shape_, tile_shape_);
    CHECK(!frame_rois.empty());
    TRACE("Allocating %llu writers", frame_rois.size());

    std::for_each(
      writers_.begin(), writers_.end(), [](ChunkWriter* w) { delete w; });
    writers_.clear();
    for (const auto& roi : frame_rois) {
        Encoder* encoder;
        if (compressor_.has_value()) {
            size_t bytes_per_chunk = get_bytes_per_chunk(
              image_shape_, tile_shape_, max_bytes_per_chunk_);
            encoder = new BloscEncoder(compressor_.value(), bytes_per_chunk);
        } else {
            size_t bytes_per_tile =
              get_bytes_per_tile(image_shape_, tile_shape_);
            encoder = new RawEncoder(bytes_per_tile);
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

void
zarr::Zarr::clear_writers_()
{
    for (auto& writer : writers_) {
        writer->close_current_file();
        delete writer;
    }
    writers_.clear();
}

void
zarr::Zarr::assign_threads_()
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

void
zarr::Zarr::recover_threads_()
{
    for (auto& writer : writers_) {
        thread_t* t = writer->release_thread();
        if (nullptr != t)
            thread_pool_.push(t);
    }
}

/// make a single pass through the frame queue and check if any writers are
/// still using any queued frames
void
zarr::Zarr::release_finished_frames_()
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

size_t
zarr::Zarr::cycle_()
{
    if (writers_.size() > thread_pool_.size())
        recover_threads_();

    release_finished_frames_();

    if (!frame_ptrs_.empty())
        assign_threads_();

    return frame_ptrs_.size();
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
           image_shape.dims.channels * tile_shape.dims.height *
           tile_shape.dims.width * tile_shape.dims.planes;
}

size_t
zarr::get_tiles_per_chunk(const ImageShape& image_shape,
                          const TileShape& tile_shape,
                          size_t max_bytes_per_chunk) noexcept
{
    return (size_t)std::floor(
      (float)max_bytes_per_chunk /
      (float)get_bytes_per_tile(image_shape, tile_shape));
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
  }
{
}

extern "C" struct Storage*
zarr_init()
{
    try {
        auto nthreads = (int)std::thread::hardware_concurrency();
        return new zarr::Zarr(std::max(nthreads - 2, 1));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
