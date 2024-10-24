#include "zarr.storage.hh"
#include "logger.h"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <stdexcept>

#define LOG(...) aq_logger(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) aq_logger(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Check failed: " #e);                     \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

#define ZARR_OK(e)                                                             \
    do {                                                                       \
        ZarrStatusCode __err = (e);                                            \
        EXPECT(__err == ZarrStatusCode_Success,                                \
               "%s",                                                           \
               Zarr_get_status_message(__err));                                \
    } while (0)

namespace sink = acquire::sink;
namespace fs = std::filesystem;

using json = nlohmann::json;

namespace {
/**
 * @brief Align a size to a given alignment.
 * @param n Size to align.
 * @param align Alignment.
 * @return Aligned size.
 */
size_t
align_up(size_t n, size_t align)
{
    EXPECT(align > 0, "Alignment must be greater than zero.");
    return align * ((n + align - 1) / align);
}

/**
 * @brief Get the filename from a StorageProperties as fs::path.
 * @param props StorageProperties for the Zarr Storage device.
 * @return fs::path representation of the Zarr data directory.
 */
fs::path
as_path(const StorageProperties* props)
{
    if (!props->uri.str) {
        return {};
    }

    std::string uri{ props->uri.str, props->uri.nbytes - 1 };

    while (uri.find("file://") != std::string::npos) {
        uri = uri.substr(7); // strlen("file://") == 7
    }

    return uri;
}

/**
 * @brief Check that the JSON string is valid. (Valid can mean empty.)
 * @param str Putative JSON metadata string.
 * @param nbytes Size of the JSON metadata char array.
 */
void
validate_json(const char* str, size_t nbytes)
{
    // Empty strings are valid (no metadata is fine).
    if (nbytes <= 1 || nullptr == str) {
        return;
    }

    std::ignore = json::parse(str,
                              str + nbytes,
                              nullptr, // callback
                              true,    // allow exceptions
                              true     // ignore comments
    );
}

/**
 * @brief Check if a URI is a web URI (e.g., for S3).
 * @param uri String to check.
 * @return True if the URI is a web URI, false otherwise.
 */
bool
is_web_uri(std::string_view uri)
{
    return uri.starts_with("http://") || uri.starts_with("https://");
}

/**
 * @brief Split a URI into its components.
 * @param uri String to split.
 * @return Vector of strings representing the components of the URI.
 */
std::vector<std::string>
split_uri(const std::string& uri)
{
    const char delim = '/';

    std::vector<std::string> out;
    size_t begin = 0, end = uri.find_first_of(delim);

    while (end != std::string::npos) {
        std::string part = uri.substr(begin, end - begin);
        if (!part.empty())
            out.push_back(part);

        begin = end + 1;
        end = uri.find_first_of(delim, begin);
    }

    // Add the last segment of the URI (if any) after the last '/'
    std::string last_part = uri.substr(begin);
    if (!last_part.empty()) {
        out.push_back(last_part);
    }

    return out;
}

/// \brief Check that the StorageProperties are valid.
/// \details Assumes either an empty or valid JSON metadata string and a
/// filename string that points to a writable directory. \param props Storage
/// properties for Zarr. \throw std::runtime_error if the parent of the Zarr
/// data directory is not an existing directory.
void
validate_props(const StorageProperties* props)
{
    EXPECT(props->uri.str, "URI string is NULL.");
    EXPECT(props->uri.nbytes, "URI string is zero size.");

    // check that JSON is correct (throw std::exception if not)
    validate_json(props->external_metadata_json.str,
                  props->external_metadata_json.nbytes);

    std::string uri{ props->uri.str, props->uri.nbytes - 1 };

    if (is_web_uri(uri)) {
        std::vector<std::string> tokens = split_uri(uri);
        CHECK(tokens.size() > 2); // http://endpoint/bucket
    } else {
        const fs::path path = as_path(props);
        fs::path parent_path = path.parent_path();
        if (parent_path.empty())
            parent_path = ".";

        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.string().c_str());

        // check directory is writable
        const auto perms = fs::status(fs::path(parent_path)).permissions();

        EXPECT((perms & (fs::perms::owner_write | fs::perms::group_write |
                         fs::perms::others_write)) != fs::perms::none,
               "Expected \"%s\" to have write permissions.",
               parent_path.c_str());
    }
}

void
validate_dimension(const struct StorageDimension* dim, bool is_append)
{
    EXPECT(dim, "Dimension is NULL.");

    if (is_append) {
        EXPECT(dim->array_size_px == 0,
               "Append dimension array size must be 0.");
    } else {
        EXPECT(dim->array_size_px > 0,
               "Dimension array size must be positive.");
    }

    EXPECT(dim->chunk_size_px > 0, "Dimension chunk size must be positive.");

    EXPECT(dim->name.str, "Dimension name is NULL.");
    EXPECT(dim->name.nbytes > 1, "Dimension name is empty.");
}

[[nodiscard]] bool
is_multiscale_supported(struct StorageDimension* dims, size_t ndims)
{
    EXPECT(dims, "Dimensions are NULL.");
    EXPECT(ndims > 2, "Expected at least 3 dimensions.");

    // 1. The final two dimensions must be space dimensions.
    if (dims[ndims - 1].kind != DimensionType_Space ||
        dims[ndims - 2].kind != DimensionType_Space) {
        return false;
    }

    // 2. Interior dimensions must have size 1
    for (auto i = 1; i < ndims - 2; ++i) {
        if (dims[i].array_size_px != 1) {
            return false;
        }
    }

    return true;
}

template<typename T>
VideoFrame*
scale_image(const uint8_t* const data,
            size_t bytes_of_data,
            const struct ImageShape& shape)
{
    CHECK(data);
    CHECK(bytes_of_data);

    const int downscale = 2;
    constexpr size_t bytes_of_type = sizeof(T);
    const auto factor = 0.25f;

    const auto width = shape.dims.width;
    const auto w_pad = width + (width % downscale);

    const auto height = shape.dims.height;
    const auto h_pad = height + (height % downscale);

    const auto size_of_image =
      static_cast<uint32_t>(w_pad * h_pad * factor * bytes_of_type);

    const size_t bytes_of_frame =
      align_up(sizeof(VideoFrame) + size_of_image, 8);

    auto* dst = (VideoFrame*)malloc(bytes_of_frame);
    CHECK(dst);
    dst->bytes_of_frame = bytes_of_frame;

    {
        dst->shape = shape;
        dst->shape.dims = {
            .width = w_pad / downscale,
            .height = h_pad / downscale,
        };
        dst->shape.strides = {
            .height = dst->shape.dims.width,
            .planes = dst->shape.dims.width * dst->shape.dims.height,
        };

        CHECK(bytes_of_image(&dst->shape) == size_of_image);
    }

    const auto* src_img = (T*)data;
    auto* dst_img = (T*)dst->data;
    memset(dst_img, 0, size_of_image);

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            const bool pad_width = (col == width - 1 && width != w_pad);

            size_t idx = row * width + col;
            dst_img[dst_idx++] =
              (T)(factor *
                  ((float)src_img[idx] +
                   (float)src_img[idx + (1 - (int)pad_width)] +
                   (float)src_img[idx + width * (1 - (int)pad_height)] +
                   (float)src_img[idx + width * (1 - (int)pad_height) +
                                  (1 - (int)pad_width)]));
        }
    }

    return dst;
}

/// @brief Average both `dst` and `src` into `dst`.
template<typename T>
void
average_two_frames(VideoFrame* dst, const VideoFrame* src)
{
    CHECK(dst);
    CHECK(src);
    CHECK(dst->bytes_of_frame == src->bytes_of_frame);

    const auto nbytes_image = bytes_of_image(&dst->shape);
    const auto num_pixels = nbytes_image / sizeof(T);
    for (auto i = 0; i < num_pixels; ++i) {
        dst->data[i] = (T)(0.5f * ((float)dst->data[i] + (float)src->data[i]));
    }
}

DeviceState
zarr_set(Storage* self_, const StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (sink::Zarr*)self_;
        self->set(props);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
        return DeviceState_AwaitingConfiguration;
    } catch (...) {
        LOGE("Exception: (unknown)");
        return DeviceState_AwaitingConfiguration;
    }

    return self_->state;
}

void
zarr_get(const Storage* self_, StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (sink::Zarr*)self_;
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
        auto* self = (sink::Zarr*)self_;
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
        auto* self = (sink::Zarr*)self_;
        self->start();
        state = self->state;
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
    DeviceState state{ DeviceState_AwaitingConfiguration };
    try {
        CHECK(self_);
        auto* self = (sink::Zarr*)self_;
        *nbytes = self->append(frames, *nbytes);
        state = self->state;
    } catch (const std::exception& exc) {
        *nbytes = 0;
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        *nbytes = 0;
        LOGE("Exception: (unknown)");
    }

    return state;
}

DeviceState
zarr_stop(Storage* self_) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };

    try {
        CHECK(self_);
        auto* self = (sink::Zarr*)self_;
        self->stop(); // state is set to DeviceState_Armed here
        state = self->state;
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
        auto* self = (sink::Zarr*)self_;
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
        auto* self = (sink::Zarr*)self_;
        self->reserve_image_shape(shape);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}
} // end ::{anonymous} namespace

sink::Zarr::Zarr(ZarrVersion version,
                 ZarrCompressionCodec compression_codec,
                 uint8_t compression_level,
                 uint8_t shuffle)
  : Storage {
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
  , version_(version)
  , store_path_()
  , custom_metadata_("{}")
  , dtype_(ZarrDataType_uint8)
  , compression_codec_(compression_codec)
  , compression_level_(compression_level)
  , compression_shuffle_(shuffle)
  , multiscale_(false)
  , stream_(nullptr)
{
    EXPECT(
      version_ < ZarrVersionCount, "Unsupported Zarr version: %d", version);
    EXPECT(compression_codec_ < ZarrCompressionCodecCount,
           "Unsupported compression codec: %d",
           compression_codec);
    EXPECT(
      compression_level_ <= 9,
      "Invalid compression level: %d. Compression level must be in [0, 9].",
      compression_level_);
    EXPECT(compression_shuffle_ <= 2,
           "Invalid shuffle value: %d. Shuffle must be 0, 1, or 2.",
           compression_shuffle_);
}

sink::Zarr::~Zarr()
{
    stop();
}

void
sink::Zarr::set(const StorageProperties* props)
{
    EXPECT(state != DeviceState_Running,
           "Cannot set properties while running.");
    EXPECT(props, "StorageProperties is NULL.");

    // check that the external metadata is valid
    if (props->external_metadata_json.str) {
        validate_json(props->external_metadata_json.str,
                      props->external_metadata_json.nbytes);

        custom_metadata_ = props->external_metadata_json.str;
    }

    if (custom_metadata_.empty()) {
        custom_metadata_ = "{}";
    }

    EXPECT(props->uri.str, "URI string is NULL.");
    EXPECT(props->uri.nbytes > 1, "URI string is empty.");
    std::string uri(props->uri.str, props->uri.nbytes - 1);

    if (is_web_uri(uri)) {
        EXPECT(props->access_key_id.str, "Access key ID is NULL.");
        EXPECT(props->access_key_id.nbytes > 1, "Access key ID is empty.");
        EXPECT(props->secret_access_key.str, "Secret access key is NULL.");
        EXPECT(props->secret_access_key.nbytes > 1,
               "Secret access key is empty.");

        auto components = split_uri(uri);
        EXPECT(components.size() > 3, "Invalid URI: %s", uri.c_str());

        s3_endpoint_ = components[0] + "//" + components[1];
        s3_bucket_name_ = components[2];
        s3_access_key_id_ = props->access_key_id.str;
        s3_secret_access_key_ = props->secret_access_key.str;

        store_path_ = components[3];
        for (auto i = 4; i < components.size(); ++i) {
            store_path_ += "/" + components[i];
        }
    } else {
        if (uri.find("file://") != std::string::npos) {
            uri = uri.substr(7); // strlen("file://") == 7
        }
        std::string store_path = uri;

        if (fs::exists(store_path)) {
            std::error_code ec;
            EXPECT(fs::remove_all(store_path, ec),
                   R"(Failed to remove folder for "%s": %s)",
                   store_path.c_str(),
                   ec.message().c_str());
        }

        fs::path parent_path = fs::path(store_path).parent_path();
        if (parent_path.empty())
            parent_path = ".";

        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.string().c_str());

        // check directory is writable
        const auto perms = fs::status(fs::path(parent_path)).permissions();

        EXPECT((perms & (fs::perms::owner_write | fs::perms::group_write |
                         fs::perms::others_write)) != fs::perms::none,
               "Expected \"%s\" to have write permissions.",
               parent_path.c_str());

        store_path_ = store_path;
    }

    dimension_names_.clear();
    dimensions_.clear();

    for (auto i = 0; i < props->acquisition_dimensions.size; ++i) {
        const auto* dim = props->acquisition_dimensions.data + i;
        validate_dimension(dim, i == 0);

        ZarrDimensionType type;
        switch (dim->kind) {
            case DimensionType_Space:
                type = ZarrDimensionType_Space;
                break;
            case DimensionType_Channel:
                type = ZarrDimensionType_Channel;
                break;
            case DimensionType_Time:
                type = ZarrDimensionType_Time;
                break;
            case DimensionType_Other:
                type = ZarrDimensionType_Other;
                break;
            default:
                throw std::runtime_error("Invalid dimension type: " +
                                         std::to_string(dim->kind));
        }

        dimension_names_.emplace_back(dim->name.str);
        dimensions_.push_back({ nullptr,
                                type,
                                dim->array_size_px,
                                dim->chunk_size_px,
                                dim->shard_size_chunks });
    }

    multiscale_ = props->enable_multiscale;

    state = DeviceState_Armed;
}

void
sink::Zarr::get(StorageProperties* props) const
{
    EXPECT(props, "StorageProperties is NULL.");

    storage_properties_destroy(props);

    std::string s3_endpoint, s3_bucket;
    std::string access_key_id, secret_access_key;

    size_t ndims;

    if (s3_endpoint_) {
        s3_endpoint = *s3_endpoint_;
    }
    if (s3_bucket_name_) {
        s3_bucket = *s3_bucket_name_;
    }
    if (s3_access_key_id_) {
        access_key_id = *s3_access_key_id_;
    }
    if (s3_secret_access_key_) {
        secret_access_key = *s3_secret_access_key_;
    }

    ndims = dimension_names_.size();

    std::string uri;
    if (!s3_endpoint.empty() && !s3_bucket.empty() && !store_path_.empty()) {
        uri = s3_endpoint + "/" + s3_bucket + "/" + store_path_;
    } else if (!store_path_.empty()) {
        uri = "file://" + fs::absolute(store_path_).string();
    }

    const size_t bytes_of_filename = uri.empty() ? 0 : uri.size() + 1;

    const char* metadata =
      custom_metadata_.empty() ? nullptr : custom_metadata_.c_str();
    const size_t bytes_of_metadata = metadata ? custom_metadata_.size() + 1 : 0;

    CHECK(storage_properties_init(props,
                                  0,
                                  uri.c_str(),
                                  bytes_of_filename,
                                  metadata,
                                  bytes_of_metadata,
                                  { 1, 1 },
                                  ndims));

    // set access key and secret
    if (!access_key_id.empty() && !secret_access_key.empty()) {
        CHECK(storage_properties_set_access_key_and_secret(
          props,
          access_key_id.c_str(),
          access_key_id.size() + 1,
          secret_access_key.c_str(),
          secret_access_key.size() + 1));
    }

    for (auto i = 0; i < ndims; ++i) {
        ZarrDimensionType dimension_type;
        size_t array_size_px, chunk_size_px, shard_size_chunks;

        auto dimension = dimensions_[i];

        auto dim_name = dimension_names_[i];
        dimension_type = dimension.type;
        array_size_px = dimension.array_size_px;
        chunk_size_px = dimension.chunk_size_px;
        shard_size_chunks = dimension.shard_size_chunks;

        DimensionType kind;
        switch (dimension_type) {
            case ZarrDimensionType_Space:
                kind = DimensionType_Space;
                break;
            case ZarrDimensionType_Channel:
                kind = DimensionType_Channel;
                break;
            case ZarrDimensionType_Time:
                kind = DimensionType_Time;
                break;
            case ZarrDimensionType_Other:
                kind = DimensionType_Other;
                break;
            default:
                throw std::runtime_error("Invalid dimension type: " +
                                         std::to_string(dimension_type));
        }

        const char* name = dim_name.empty() ? nullptr : dim_name.c_str();
        const size_t nbytes = name ? dim_name.size() + 1 : 0;

        CHECK(storage_properties_set_dimension(props,
                                               i,
                                               name,
                                               nbytes,
                                               kind,
                                               array_size_px,
                                               chunk_size_px,
                                               shard_size_chunks));
    }

    CHECK(storage_properties_set_enable_multiscale(props, multiscale_));
}

void
sink::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    memset(meta, 0, sizeof(*meta));

    meta->chunking_is_supported = 1;
    meta->multiscale_is_supported = 1;
    meta->s3_is_supported = 1;
    meta->sharding_is_supported =
      static_cast<uint8_t>(version_ == ZarrVersion_3);
}

void
sink::Zarr::start()
{
    EXPECT(state == DeviceState_Armed, "Device is not armed.");

    if (stream_) {
        ZarrStream_destroy(stream_);
        stream_ = nullptr;
    }

    for (auto i = 0; i < dimension_names_.size(); ++i) {
        auto& dim = dimensions_[i];
        dim.name = dimension_names_[i].c_str();
    }

    ZarrStreamSettings stream_settings{
        .store_path = store_path_.c_str(),
        .custom_metadata = custom_metadata_.c_str(),
        .s3_settings = nullptr,
        .compression_settings = nullptr,
        .dimensions = dimensions_.data(),
        .dimension_count = dimensions_.size(),
        .multiscale = multiscale_,
        .data_type = dtype_,
        .version = version_,
    };

    ZarrS3Settings s3_settings;
    ZarrCompressionSettings compression_settings;

    if (s3_endpoint_ && s3_bucket_name_ && s3_access_key_id_ &&
        s3_secret_access_key_) {
        s3_settings = { .endpoint = s3_endpoint_->c_str(),
                        .bucket_name = s3_bucket_name_->c_str(),
                        .access_key_id = s3_access_key_id_->c_str(),
                        .secret_access_key = s3_secret_access_key_->c_str() };

        stream_settings.s3_settings = &s3_settings;
    }

    if (compression_codec_ > ZarrCompressionCodec_None) {
        compression_settings = {
            .compressor = ZarrCompressor_Blosc1,
            .codec = compression_codec_,
            .level = compression_level_,
            .shuffle = compression_shuffle_,
        };

        stream_settings.compression_settings = &compression_settings;
    }

    stream_ = ZarrStream_create(&stream_settings);
    CHECK(stream_);

    state = DeviceState_Running;
}

void
sink::Zarr::stop() noexcept
{
    if (DeviceState_Running == state) {
        // make a copy of current settings before destroying the stream
        state = DeviceState_Armed;

        ZarrStream_destroy(stream_);
        stream_ = nullptr;
    }
}

size_t
sink::Zarr::append(const VideoFrame* frames, size_t nbytes)
{
    EXPECT(DeviceState_Running == state, "Device is not running.");

    if (0 == nbytes) {
        return nbytes;
    }

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end; cur = next()) {
        const size_t bytes_of_frame = bytes_of_image(&cur->shape);
        size_t bytes_written;
        ZARR_OK(ZarrStream_append(
          stream_, cur->data, bytes_of_frame, &bytes_written));
        EXPECT(bytes_written == bytes_of_frame,
               "Expected to write %zu bytes, but wrote %zu.",
               bytes_of_frame,
               bytes_written);
    }

    return nbytes;
}

void
sink::Zarr::reserve_image_shape(const ImageShape* shape)
{
    EXPECT(state == DeviceState_Armed, "Device is not armed.");

    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);

    EXPECT(dimensions_.size() > 2, "Expected at least 3 dimensions.");

    // check that the configured dimensions match the image shape
    {
        ZarrDimensionProperties y_dim = dimensions_[dimensions_.size() - 2];
        EXPECT(y_dim.array_size_px == shape->dims.height,
               "Image height mismatch.");

        ZarrDimensionProperties x_dim = dimensions_.back();
        EXPECT(x_dim.array_size_px == shape->dims.width,
               "Image width mismatch.");
    }

    switch (shape->type) {
        case SampleType_u8:
            dtype_ = ZarrDataType_uint8;
            break;
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            dtype_ = ZarrDataType_uint16;
            break;
        case SampleType_i8:
            dtype_ = ZarrDataType_int8;
            break;
        case SampleType_i16:
            dtype_ = ZarrDataType_int16;
            break;
        case SampleType_f32:
            dtype_ = ZarrDataType_float32;
            break;
        default:
            throw std::runtime_error("Unsupported image type: " +
                                     std::to_string(shape->type));
    }
}

extern "C"
{
    struct Storage* zarr_v2_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_2, ZarrCompressionCodec_None, 0, 0);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }
    struct Storage* compressed_zarr_v2_zstd_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_2, ZarrCompressionCodec_BloscZstd, 1, 1);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }

    struct Storage* compressed_zarr_v2_lz4_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_2, ZarrCompressionCodec_BloscLZ4, 1, 1);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }

    struct Storage* zarr_v3_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_3, ZarrCompressionCodec_None, 0, 0);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }
    struct Storage* compressed_zarr_v3_zstd_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_3, ZarrCompressionCodec_BloscZstd, 1, 1);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }

    struct Storage* compressed_zarr_v3_lz4_init()
    {
        try {
            return new sink::Zarr(
              ZarrVersion_3, ZarrCompressionCodec_BloscLZ4, 1, 1);
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }
} // extern "C"