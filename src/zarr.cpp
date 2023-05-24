#include "zarr.hh"

#include "device/kit/storage.h"
#include "logger.h"
#include "platform.h"
#include "zarr.raw.hh"

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
      .reserve_image_shape = ::zarr_reserve_image_shape,
  }
{
}

extern "C" struct Storage*
zarr_init()
{
    try {
        auto nthreads = (int)std::thread::hardware_concurrency();
        return new zarr::Zarr<zarr::RawEncoder>(std::max(nthreads - 2, 1));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
