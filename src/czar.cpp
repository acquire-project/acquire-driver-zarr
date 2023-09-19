#include "czar.hh"

#include "writers/chunk.writer.hh"
#include "json.hpp"

namespace zarr = acquire::sink::zarr;
using json = nlohmann::json;

namespace {
/// \brief Check that the JSON string is valid. (Valid can mean empty.)
/// \param str Putative JSON metadata string.
/// \param nbytes Size of the JSON metadata char array
void
validate_json(const char* str, size_t nbytes)
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

/// \brief Get the filename from a StorageProperties as fs::path.
/// \param props StorageProperties for the Zarr Storage device.
/// \return fs::path representation of the Zarr data directory.
fs::path
as_path(const StorageProperties& props)
{
    return { props.filename.str,
             props.filename.str + props.filename.nbytes - 1 };
}

/// \brief Check that the StorageProperties are valid.
/// \details Assumes either an empty or valid JSON metadata string and a
/// filename string that points to a writable directory. \param props Storage
/// properties for Zarr. \throw std::runtime_error if the parent of the Zarr
/// data directory is not an existing directory.
void
validate_props(const StorageProperties* props)
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

        // check directory is writable
        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.c_str());

        const auto perms = fs::status(fs::path(parent_path)).permissions();

        EXPECT((perms & (fs::perms::owner_write | fs::perms::group_write |
                         fs::perms::others_write)) != fs::perms::none,
               "Expected \"%s\" to have write permissions.",
               parent_path.c_str());
    }
}

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
} // end ::{anonymous} namespace

/// StorageInterface
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

zarr::Czar::Czar(BloscCompressionParams&& compression_params)
{
    compression_params_ = std::move(compression_params);
}

void
zarr::Czar::set(const StorageProperties* props)
{
    CHECK(props);

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    // checks the directory exists and is writable
    validate_props(props);
    dataset_root_ = as_path(*props);

    if (props->external_metadata_json.str)
        external_metadata_json_ = props->external_metadata_json.str;

    pixel_scale_um_ = props->pixel_scale_um;

    // chunking
    image_tile_shapes_.clear();
    image_tile_shapes_.emplace_back();

    set_chunking(props->chunking, meta.chunking);

    if (props->enable_multiscale && !meta.multiscale.supported) {
        LOGE("OME-Zarr multiscale not yet supported by this version of Zarr.");
    }
    enable_multiscale_ = meta.multiscale.supported && props->enable_multiscale;
}

void
zarr::Czar::get(StorageProperties* props) const
{
    CHECK(storage_properties_set_filename(
      props, dataset_root_.string().c_str(), dataset_root_.string().size()));
    CHECK(storage_properties_set_external_metadata(
      props, external_metadata_json_.c_str(), external_metadata_json_.size()));
    props->pixel_scale_um = pixel_scale_um_;

    if (!image_tile_shapes_.empty()) {
        props->chunking.tile.width = image_tile_shapes_.at(0).second.cols;
        props->chunking.tile.height = image_tile_shapes_.at(0).second.rows;
    }
    props->chunking.tile.planes = 1;

    props->enable_multiscale = enable_multiscale_;
}

void
zarr::Czar::start()
{
    if (fs::exists(dataset_root_)) {
        std::error_code ec;
        EXPECT(fs::remove_all(dataset_root_, ec),
               R"(Failed to remove folder for "%s": %s)",
               dataset_root_.c_str(),
               ec.message().c_str());
    }
    fs::create_directories(dataset_root_);

    write_base_metadata_();
    write_group_metadata_();
    write_all_array_metadata_();
    write_external_metadata_();
}

int
zarr::Czar::stop() noexcept
{
    if (DeviceState_Running == state) {
        state = DeviceState_Armed;
    }

    write_all_array_metadata_(); // must precede close of chunk file
    write_group_metadata_();     // write multiscales metadata, if applicable

    for (auto& writer : writers_) {
        writer->finalize();
    }
    writers_.clear();
    return 1;
}

size_t
zarr::Czar::append(const VideoFrame* frames, size_t nbytes)
{
    if (0 == nbytes) {
        return nbytes;
    }

    using namespace acquire::sink::zarr;

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end; cur = next()) {
        for (auto& writer : writers_) {
            CHECK(writer->write(cur));
        }
    }
    return nbytes;
}

void
zarr::Czar::reserve_image_shape(const ImageShape* shape)
{
    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);
    image_tile_shapes_.at(0).first = {
        .cols = shape->dims.width,
        .rows = shape->dims.height,
    };
    pixel_type_ = shape->type;

    ImageDims& image_shape = image_tile_shapes_.at(0).first;
    ImageDims& tile_shape = image_tile_shapes_.at(0).second;

    // ensure that tile dimensions are compatible with the image shape
    {
        StorageProperties props = { 0 };
        get(&props);
        uint32_t tile_width = props.chunking.tile.width;
        if (image_shape.cols > 0 &&
            (tile_width == 0 || tile_width > image_shape.cols)) {
            LOGE("%s. Setting width to %u.",
                 tile_width == 0 ? "Tile width not specified"
                                 : "Specified tile width is too large",
                 image_shape.cols);
            tile_width = image_shape.cols;
        }
        tile_shape.cols = tile_width;

        uint32_t tile_height = props.chunking.tile.height;
        if (image_shape.rows > 0 &&
            (tile_height == 0 || tile_height > image_shape.rows)) {
            LOGE("%s. Setting height to %u.",
                 tile_height == 0 ? "Tile height not specified"
                                  : "Specified tile height is too large",
                 image_shape.rows);
            tile_height = image_shape.rows;
        }
        tile_shape.rows = tile_height;

        storage_properties_destroy(&props);
    }

    // ensure that the chunk size can accommodate at least one tile
    uint64_t bytes_per_tile = common::bytes_per_tile(tile_shape, pixel_type_);
    CHECK(bytes_per_tile > 0);

    if (max_bytes_per_chunk_ < bytes_per_tile) {
        LOGE("Specified chunk size %llu is too small. Setting to %llu bytes.",
             max_bytes_per_chunk_,
             bytes_per_tile);
        max_bytes_per_chunk_ = bytes_per_tile;
    }

    allocate_writers_();
}

/// Czar

void
zarr::Czar::set_chunking(const ChunkingProps& props, const ChunkingMeta& meta)
{
    max_bytes_per_chunk_ = std::clamp(props.max_bytes_per_chunk,
                                      (uint64_t)meta.max_bytes_per_chunk.low,
                                      (uint64_t)meta.max_bytes_per_chunk.high);

    image_tile_shapes_.at(0).second = {
        .cols = props.tile.width,
        .rows = props.tile.height,
    };
}

void
zarr::Czar::write_all_array_metadata_() const
{
    namespace fs = std::filesystem;

    for (auto i = 0; i < image_tile_shapes_.size(); ++i) {
        const auto image_shape = image_tile_shapes_.at(i).first;
        const auto tile_shape = image_tile_shapes_.at(i).second;

        write_array_metadata_(i, image_shape, tile_shape);
    }
}

#ifndef NO_UNIT_TESTS

#endif
