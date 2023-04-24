#ifndef H_ACQUIRE_STORAGE_ZARR_V0
#define H_ACQUIRE_STORAGE_ZARR_V0

#include "device/kit/storage.h"
#include "platform.h"
#include "logger.h"

#include "prelude.h"
#include "zarr.raw.hh"
#include "zarr.codec.hh"

#include <string>
#include <optional>
#include <filesystem>
#include <algorithm>
#include "json.hpp"

#ifndef __cplusplus
#error "This header requires C++20"
#endif

namespace acquire::sink::zarr {

// StorageInterface

struct StorageInterface : public Storage
{
    StorageInterface();
    virtual ~StorageInterface() = default;
    virtual void set(const StorageProperties* props) = 0;
    virtual void get(StorageProperties* props) const = 0;
    virtual void start() = 0;
    virtual int stop() noexcept = 0;

    /// @return number of consumed bytes
    virtual size_t append(const VideoFrame* frames, size_t nbytes) = 0;
};

/// \brief Zarr writer that conforms to v0.4 of the OME-NGFF specification.
///
/// This writes one multi-scale zarr image with one level/scale using the
/// OME-NGFF specification to determine the directory structure and contents
/// of group and array attributes.
///
/// https://ngff.openmicroscopy.org/0.4/
template<Writer ChunkWriter>
struct Zarr final : StorageInterface
{
    Zarr();
    ~Zarr() override;

    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void start() override;
    [[nodiscard]] int stop() noexcept override;

    /// @return number of consumed bytes
    size_t append(const VideoFrame* frames, size_t nbytes) override;

  private:
    // static - set on construction
    char dimension_separator_;

    // changes on set()
    std::string data_dir_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    size_t bytes_per_chunk_;

    // changes during acquisition
    ImageShape image_shape_;
    Maybe<ChunkWriter> current_chunk_file_;
    size_t frame_count_;

    void create_data_directory_() const;
    void create_data_file_();
    void write_zarray_json_() const;
    void write_external_metadata_json_() const;
    void write_zgroup_json_() const;
    void write_group_zattrs_json_() const;
    void rollover_();
    void fill_zeros_();

    [[nodiscard]] std::string get_chunk_file_path_(const ImageShape& shape,
                                                   size_t bytes_per_chunk,
                                                   size_t frame_count);
    [[nodiscard]] static size_t get_frames_per_chunk_(
      const ImageShape& shape,
      size_t bytes_per_chunk) noexcept;
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
get_bytes_per_frame_(const ImageShape& image_shape) noexcept;

void
write_string(const std::string& path, const std::string& str);

} // namespace acquire::storage::zarr

//
// IMPLEMENTATION
//

template<acquire::sink::zarr::Writer ChunkWriter>
acquire::sink::zarr::Zarr<ChunkWriter>::Zarr()
  : dimension_separator_{ '/' }
  , image_shape_{ 0 }
  , frame_count_{ 0 }
  , pixel_scale_um_{ 1, 1 }
{
}

template<acquire::sink::zarr::Writer ChunkWriter>
acquire::sink::zarr::Zarr<ChunkWriter>::~Zarr()
{
    if (!stop())
        LOGE("Failed to stop on destruct!");
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::set(const StorageProperties* props)
{
    using namespace acquire::sink::zarr;
    CHECK(props);

    // checks the directory exists and is writable
    validate_props(props);
    data_dir_ = as_path(*props).string();

    if (props->external_metadata_json.str)
        external_metadata_json_ = props->external_metadata_json.str;

    pixel_scale_um_ = props->pixel_scale_um;
    bytes_per_chunk_ = props->chunking.bytes_per_chunk;

    // Use 16 MB default chunk size if 0 is passed in.
    bytes_per_chunk_ = bytes_per_chunk_ ? bytes_per_chunk_ : (1ULL << 24);
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::get(StorageProperties* props) const
{
    CHECK(Device_Ok == storage_properties_set_filename(
                         props, data_dir_.c_str(), data_dir_.size()));
    CHECK(Device_Ok == storage_properties_set_external_metadata(
                         props,
                         external_metadata_json_.c_str(),
                         external_metadata_json_.size()));
    props->pixel_scale_um = pixel_scale_um_;
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::start()
{
    frame_count_ = 0;
    create_data_directory_();
    write_zgroup_json_();
    write_group_zattrs_json_();
    create_data_file_();
    write_zarray_json_();
    write_external_metadata_json_();
}

template<acquire::sink::zarr::Writer ChunkWriter>
int
acquire::sink::zarr::Zarr<ChunkWriter>::stop() noexcept
{
    int is_ok = 1;

    if (DeviceState_Running == state) {
        state = DeviceState_Armed;

        try {
            write_zarray_json_(); // must precede close of chunk file
            fill_zeros_();
            current_chunk_file_.close();
            LOG("Zarr: Writer stop");
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
            is_ok = 0;
        } catch (...) {
            LOGE("Exception: (unknown)");
            is_ok = 0;
        }
    }

    return is_ok;
}

template<acquire::sink::zarr::Writer ChunkWriter>
size_t
acquire::sink::zarr::Zarr<ChunkWriter>::append(const VideoFrame* frames,
                                               size_t nbytes)
{
    using namespace acquire::sink::zarr;

    if (0 == nbytes)
        return nbytes;

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end; cur = next()) {
        const auto bytes_of_image = cur->bytes_of_frame - sizeof(*cur);

        // handle incoming image shape
        if (0 == frame_count_) {
            image_shape_ = cur->shape;
        } else {
            validate_image_shapes_equal(image_shape_, cur->shape);
        }
        current_chunk_file_.set_bytes_per_pixel(
          bytes_per_sample_type(image_shape_.type));

        write_all(current_chunk_file_, cur->data, cur->data + bytes_of_image);

        ++frame_count_;

        size_t frames_per_chunk =
          get_frames_per_chunk_(image_shape_, bytes_per_chunk_);
        if (frames_per_chunk <= 1)
            LOG("WARNING: Chunk size (%f MB is too small for image shape "
                "(%u, %u)x%u %sx%u",
                bytes_per_chunk_ * 1e-6,
                image_shape_.dims.width,
                image_shape_.dims.height,
                image_shape_.dims.planes,
                sample_type_to_string(image_shape_.type),
                image_shape_.dims.channels);
        CHECK(frames_per_chunk > 0);
        if (0 == frame_count_ % frames_per_chunk)
            rollover_();
    }
    return nbytes;
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::create_data_directory_() const
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

/// \brief Creates the data file for the current chunk.
template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::create_data_file_()
{
    namespace fs = std::filesystem;
    const auto data_file_path =
      get_chunk_file_path_(image_shape_, bytes_per_chunk_, frame_count_);

    fs::create_directories(fs::path(data_file_path).parent_path());

    current_chunk_file_.create(bytes_per_chunk_, data_file_path);
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::write_zarray_json_() const
{
    namespace fs = std::filesystem;
    using namespace acquire::sink::zarr;
    using json = nlohmann::json;

    const auto frames_per_chunk = std::min(
      frame_count_, get_frames_per_chunk_(image_shape_, bytes_per_chunk_));

    // TODO: currently do struct -> string -> json -> string,
    // instead do struct -> json -> string.
    const auto compressor_json = current_chunk_file_.to_json();

    const json zarray_attrs = {
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
            image_shape_.dims.channels,
            image_shape_.dims.height,
            image_shape_.dims.width,
          } },
        { "dtype", sample_type_to_dtype(image_shape_.type) },
        { "compressor", json::parse(compressor_json) },
        { "fill_value", 0 },
        { "order", "C" },
        { "filters", nullptr },
        { "dimension_separator", std::string(1, dimension_separator_) },
    };

    std::string zarray_path = (fs::path(data_dir_) / "0" / ".zarray").string();
    write_string(zarray_path, zarray_attrs.dump());
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::write_external_metadata_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (fs::path(data_dir_) / "0" / ".zattrs").string();
    write_string(zattrs_path, external_metadata_json_);
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::write_group_zattrs_json_() const
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

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::write_zgroup_json_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (fs::path(data_dir_) / ".zgroup").string();
    write_string(zgroup_path, zgroup.dump());
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::rollover_()
{
    current_chunk_file_.close();
    create_data_file_();
}

template<acquire::sink::zarr::Writer ChunkWriter>
void
acquire::sink::zarr::Zarr<ChunkWriter>::fill_zeros_()
{
    const auto frames_per_chunk =
      get_frames_per_chunk_(image_shape_, bytes_per_chunk_);
    if (0 == frames_per_chunk || frame_count_ < frames_per_chunk ||
        0 == frame_count_ % frames_per_chunk)
        return;

    const auto frames_to_fill =
      ((frame_count_ / frames_per_chunk) + 1) * frames_per_chunk - frame_count_;
    const auto bytes_to_fill =
      get_bytes_per_frame_(image_shape_) * frames_to_fill;
    current_chunk_file_.set_bytes_per_pixel(
      bytes_per_sample_type(image_shape_.type));

    // Write a bunch of zeros 4k at a time.
    size_t written = 0;
    while (written < bytes_to_fill) {
        uint8_t buf[1 << 12] = { 0 };
        size_t n = std::min(sizeof(buf), bytes_to_fill - written);
        written += current_chunk_file_.write(buf, buf + n);
    }
}

/// \brief Computes the chunk file path using the `image_shape` and
/// `frame_count`
template<acquire::sink::zarr::Writer ChunkWriter>
std::string
acquire::sink::zarr::Zarr<ChunkWriter>::get_chunk_file_path_(
  const ImageShape& shape,
  size_t bytes_per_chunk,
  size_t frame_count)
{
    namespace fs = std::filesystem;
    size_t chunk = 0;
    if (frame_count > 0) {
        const size_t frames_per_chunk =
          get_frames_per_chunk_(shape, bytes_per_chunk);
        CHECK(frames_per_chunk > 0);
        chunk = frame_count / frames_per_chunk;
    }
    char data_file[256] = {};
    snprintf(data_file,
             sizeof(data_file),
             "0%c%llu%c0%c0%c0",
             dimension_separator_,
             (unsigned long long)chunk,
             dimension_separator_,
             dimension_separator_,
             dimension_separator_);

    return (fs::path(data_dir_) / data_file).string();
}

/// \brief Compute the number of frames in a chunk, using `image_shape_` and
/// bound on the number of bytes per chunk.
/// \return The number of frames to expect in a chunk.
template<acquire::sink::zarr::Writer ChunkWriter>
size_t
acquire::sink::zarr::Zarr<ChunkWriter>::get_frames_per_chunk_(
  const ImageShape& shape,
  size_t bytes_per_chunk) noexcept
{
    const auto bytes_per_frame = get_bytes_per_frame_(shape);
    return bytes_per_frame == 0
             ? 1
             : std::max(size_t(1), bytes_per_chunk / bytes_per_frame);
}

#endif // H_ACQUIRE_STORAGE_ZARR_V0
