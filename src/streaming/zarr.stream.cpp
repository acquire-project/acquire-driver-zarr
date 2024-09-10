#include "zarr.stream.hh"
#include "zarr.h"
#include "logger.hh"
#include "s3.connection.hh"
#include "zarrv2.array.writer.hh"
#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <string_view>

#define EXPECT_VALID_ARGUMENT(e, ...)                                          \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrStatus_InvalidArgument;                                  \
        }                                                                      \
    } while (0)

#define STREAM_GET_STRING(stream, member)                                      \
    do {                                                                       \
        if (!stream) {                                                         \
            LOG_ERROR("Null pointer: %s", #stream);                            \
            return nullptr;                                                    \
        }                                                                      \
        return stream->settings().member.c_str();                              \
    } while (0)

namespace fs = std::filesystem;

namespace {
bool
is_s3_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return !settings->s3_endpoint.empty() &&
           !settings->s3_bucket_name.empty() &&
           !settings->s3_access_key_id.empty() &&
           !settings->s3_secret_access_key.empty();
}

bool
validate_settings(const struct ZarrStreamSettings_s* settings,
                  ZarrVersion version)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return false;
    }
    if (version < ZarrVersion_2 || version >= ZarrVersionCount) {
        LOG_ERROR("Invalid Zarr version: %d", version);
        return false;
    }

    std::string store_path(settings->store_path);
    std::string s3_endpoint(settings->s3_endpoint);
    std::string s3_bucket_name(settings->s3_bucket_name);
    std::string s3_access_key_id(settings->s3_access_key_id);
    std::string s3_secret_access_key(settings->s3_secret_access_key);

    // we require the store_path to be nonempty
    if (store_path.empty()) {
        LOG_ERROR("Store path is empty");
        return false;
    }

    // if all S3 settings are nonempty, we consider this an S3 store
    if (is_s3_acquisition(settings)) {
        // check that the S3 endpoint is a valid URL
        if (s3_endpoint.find("http://") != 0 &&
            s3_endpoint.find("https://") != 0) {
            LOG_ERROR("Invalid S3 endpoint: %s", s3_endpoint.c_str());
            return false;
        }

        // test the S3 connection
        try {
            zarr::S3Connection connection(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (!connection.check_connection()) {
                LOG_ERROR("Connection to '%s' failed", s3_endpoint.c_str());
                return false;
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating S3 connection: %s", e.what());
            return false;
        }
    } else {
        // if any S3 setting is nonempty, this is a filesystem store
        fs::path path(store_path);
        fs::path parent_path = path.parent_path();
        if (parent_path.empty()) {
            parent_path = ".";
        }

        // parent path must exist and be a directory
        if (!fs::exists(parent_path) || !fs::is_directory(parent_path)) {
            LOG_ERROR("Parent path '%s' does not exist or is not a directory",
                      parent_path.c_str());
            return false;
        }

        // parent path must be writable
        const auto perms = fs::status(parent_path).permissions();
        const bool is_writable =
          (perms & (fs::perms::owner_write | fs::perms::group_write |
                    fs::perms::others_write)) != fs::perms::none;

        if (!is_writable) {
            LOG_ERROR("Parent path '%s' is not writable", parent_path.c_str());
            return false;
        }
    }

    if (settings->dtype >= ZarrDataTypeCount) {
        LOG_ERROR("Invalid data type: %d", settings->dtype);
        return false;
    }

    if (settings->compressor >= ZarrCompressorCount) {
        LOG_ERROR("Invalid compressor: %d", settings->compressor);
        return false;
    }

    if (settings->compression_codec >= ZarrCompressionCodecCount) {
        LOG_ERROR("Invalid compression codec: %d", settings->compression_codec);
        return false;
    }

    // if compressing, we require a compression codec
    if (settings->compressor != ZarrCompressor_None &&
        settings->compression_codec == ZarrCompressionCodec_None) {
        LOG_ERROR("Compression codec must be set when using a compressor");
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < settings->dimensions.size(); ++i) {
        if (!validate_dimension(settings->dimensions[i])) {
            LOG_ERROR("Invalid dimension at index %d", i);
            return false;
        }

        if (i > 0 && settings->dimensions[i].array_size_px == 0) {
            LOG_ERROR("Only the first dimension can have an array size of 0");
            return false;
        }
    }

    // if version 3, we require shard size to be positive
    if (version == ZarrVersion_3) {
        for (const auto& dim : settings->dimensions) {
            if (dim.shard_size_chunks == 0) {
                LOG_ERROR("Shard sizes must be positive");
                return false;
            }
        }
    }

    return true;
}

const char*
dimension_type_to_string(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Time:
            return "time";
        case ZarrDimensionType_Channel:
            return "channel";
        case ZarrDimensionType_Space:
            return "space";
        case ZarrDimensionType_Other:
            return "other";
        default:
            return "(unknown)";
    }
}

template<typename T>
uint8_t*
scale_image(const uint8_t* const src,
            size_t& bytes_of_src,
            size_t& width,
            size_t& height)
{
    CHECK(src);
    
    const size_t bytes_of_frame = width * height * sizeof(T);
    EXPECT(bytes_of_src >= bytes_of_frame,
           "Expecting at least %zu bytes, got %zu",
           bytes_of_frame,
           bytes_of_src);

    const int downscale = 2;
    constexpr size_t bytes_of_type = sizeof(T);
    const double factor = 0.25;

    const auto w_pad = width + (width % downscale);
    const auto h_pad = height + (height % downscale);

    const auto size_downscaled =
      static_cast<uint32_t>(w_pad * h_pad * factor * bytes_of_type);

    auto* dst = new uint8_t[size_downscaled];
    EXPECT(dst,
           "Failed to allocate %zu bytes for destination frame",
           size_downscaled);

    memset(dst, 0, size_downscaled);

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            size_t src_idx = row * width + col;
            const bool pad_width = (col == width - 1 && width != w_pad);

            double here = src[src_idx];
            double right = src[src_idx + (1 - static_cast<int>(pad_width))];
            double down =
              src[src_idx + width * (1 - static_cast<int>(pad_height))];
            double diag =
              src[src_idx + width * (1 - static_cast<int>(pad_height)) +
                  (1 - static_cast<int>(pad_width))];

            dst[dst_idx++] =
              static_cast<T>(factor * (here + right + down + diag));
        }
    }

    bytes_of_src = size_downscaled;
    width = w_pad / 2;
    height = h_pad / 2;

    return dst;
}

template<typename T>
void
average_two_frames(void* dst_,
                   size_t bytes_of_dst,
                   const void* src_,
                   size_t bytes_of_src)
{
    CHECK(dst_);
    CHECK(src_);
    EXPECT(bytes_of_dst == bytes_of_src,
           "Expecting %zu bytes in destination, got %zu",
           bytes_of_src,
           bytes_of_dst);

    T* dst = static_cast<T*>(dst_);
    const T* src = static_cast<const T*>(src_);

    const auto num_pixels = bytes_of_src / sizeof(T);
    for (auto i = 0; i < num_pixels; ++i) {
        dst[i] = static_cast<T>(0.5 * (dst[i] + src[i]));
    }
}
} // namespace

size_t
zarr::bytes_of_type(ZarrDataType data_type)
{
    switch (data_type) {
        case ZarrDataType_int8:
        case ZarrDataType_uint8:
            return 1;
        case ZarrDataType_int16:
        case ZarrDataType_uint16:
        case ZarrDataType_float16:
            return 2;
        case ZarrDataType_int32:
        case ZarrDataType_uint32:
        case ZarrDataType_float32:
            return 4;
        case ZarrDataType_int64:
        case ZarrDataType_uint64:
        case ZarrDataType_float64:
            return 8;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(data_type));
    }
}

size_t
zarr::bytes_of_frame(const std::vector<Dimension>& dims, ZarrDataType type)
{
    return bytes_of_type(type) * dims.back().array_size_px *
           dims[dims.size() - 2].array_size_px;
}

extern "C"
{
    ZarrStream* ZarrStream_create(struct ZarrStreamSettings_s* settings,
                                  ZarrVersion version)
    {
        if (!validate_settings(settings, version)) {
            return nullptr;
        }

        // initialize the stream
        ZarrStream_s* stream;

        try {
            stream = new ZarrStream(settings, version);
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for Zarr stream");
            return nullptr;
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating Zarr stream: %s", e.what());
            return nullptr;
        }
        ZarrStreamSettings_destroy(settings);

        return stream;
    }

    void ZarrStream_destroy(ZarrStream* stream)
    {
        delete stream;
    }

    /* Appending data */

    ZarrStatus ZarrStream_append(ZarrStream* stream,
                                const void* data,
                                size_t bytes_in,
                                size_t* bytes_out)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(data, "Null pointer: data");
        EXPECT_VALID_ARGUMENT(bytes_out, "Null pointer: bytes_out");

        try {
            *bytes_out = stream->append(data, bytes_in);
        } catch (const std::exception& e) {
            LOG_ERROR("Error appending data: %s", e.what());
            return ZarrStatus_InternalError;
        }

        return ZarrStatus_Success;
    }

    /* Getters */

    ZarrVersion ZarrStream_get_version(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning ZarrVersion_2");
            return ZarrVersion_2;
        }
        return static_cast<ZarrVersion>(stream->version());
    }

    const char* ZarrStream_get_store_path(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, store_path);
    }

    const char* ZarrStream_get_s3_endpoint(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, s3_endpoint);
    }

    const char* ZarrStream_get_s3_bucket_name(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, s3_bucket_name);
    }

    const char* ZarrStream_get_s3_access_key_id(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, s3_access_key_id);
    }

    const char* ZarrStream_get_s3_secret_access_key(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, s3_secret_access_key);
    }

    const char* ZarrStream_get_external_metadata(const ZarrStream* stream)
    {
        STREAM_GET_STRING(stream, external_metadata);
    }

    ZarrCompressor ZarrStream_get_compressor(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning ZarrCompressor_None");
            return ZarrCompressor_None;
        }
        return ZarrStreamSettings_get_compressor(&stream->settings());
    }

    ZarrCompressionCodec ZarrStream_get_compression_codec(
      const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING(
              "Null pointer: stream. Returning ZarrCompressionCodec_None");
            return ZarrCompressionCodec_None;
        }
        return ZarrStreamSettings_get_compression_codec(&stream->settings());
    }

    uint8_t ZarrStream_get_compression_level(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning 0");
            return 0;
        }
        return ZarrStreamSettings_get_compression_level(&stream->settings());
    }

    uint8_t ZarrStream_get_compression_shuffle(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning 0");
            return 0;
        }
        return ZarrStreamSettings_get_compression_shuffle(&stream->settings());
    }

    size_t ZarrStream_get_dimension_count(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning 0");
            return 0;
        }
        return ZarrStreamSettings_get_dimension_count(&stream->settings());
    }

    ZarrStatus ZarrStream_get_dimension(const ZarrStream* stream,
                                       size_t index,
                                       char* name,
                                       size_t bytes_of_name,
                                       ZarrDimensionType* kind,
                                       size_t* array_size_px,
                                       size_t* chunk_size_px,
                                       size_t* shard_size_chunks)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        return ZarrStreamSettings_get_dimension(&stream->settings(),
                                                index,
                                                name,
                                                bytes_of_name,
                                                kind,
                                                array_size_px,
                                                chunk_size_px,
                                                shard_size_chunks);
    }

    uint8_t ZarrStream_get_multiscale(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning 0");
            return 0;
        }
        return ZarrStreamSettings_get_multiscale(&stream->settings());
    }

    ZarrStreamSettings* ZarrStream_get_settings(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning nullptr");
            return nullptr;
        }

        return ZarrStreamSettings_copy(&stream->settings());
    }

    /* Logging */

    ZarrStatus Zarr_set_log_level(LogLevel level)
    {
        if (level < LogLevel_Debug || level >= LogLevelCount) {
            return ZarrStatus_InvalidArgument;
        }

        Logger::set_log_level(level);
        return ZarrStatus_Success;
    }

    LogLevel Zarr_get_log_level()
    {
        return Logger::get_log_level();
    }

    /* Error handling */

    const char* Zarr_get_error_message(ZarrStatus status)
    {
        switch (status) {
            case ZarrStatus_Success:
                return "Success";
            case ZarrStatus_InvalidArgument:
                return "Invalid argument";
            case ZarrStatus_Overflow:
                return "Overflow";
            case ZarrStatus_InvalidIndex:
                return "Invalid index";
            case ZarrStatus_NotYetImplemented:
                return "Not yet implemented";
            case ZarrStatus_InternalError:
                return "Internal status";
            default:
                return "Unknown status";
        }
    }
}

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings, uint8_t version)
  : settings_(*settings)
  , version_(version)
  , error_()
{
    settings_.dimensions = std::move(settings->dimensions);

    // spin up thread pool
    thread_pool_ = std::make_shared<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error_(err); });

    // create the data store
    EXPECT(create_store_(), "%s", error_.c_str());

    // allocate writers
    EXPECT(create_writers_(), "%s", error_.c_str());

    // allocate multiscale frame placeholders
    create_scaled_frames_();

    // allocate metadata sinks
    EXPECT(create_metadata_sinks_(), "%s", error_.c_str());

    // write base metadata
    EXPECT(write_base_metadata_(), "%s", error_.c_str());

    // write group metadata
    EXPECT(write_group_metadata_(), "%s", error_.c_str());

    // write external metadata
    EXPECT(write_external_metadata_(), "%s", error_.c_str());
}

ZarrStream_s::~ZarrStream_s()
{
    try {
        // must precede close of chunk file
        write_group_metadata_();
        metadata_sinks_.clear();

        writers_.clear(); // flush before shutting down thread pool
        thread_pool_->await_stop();

        for (auto& [_, frame] : scaled_frames_) {
            if (frame) {
                delete[] *frame;
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Error finalizing Zarr stream: %s", e.what());
    }
}

size_t
ZarrStream_s::append(const void* data_, size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: %s", error_.c_str());

    if (0 == nbytes) {
        return 0;
    }

    const size_t bytes_of_frame = zarr::bytes_of_frame(
      settings_.dimensions, static_cast<ZarrDataType>(settings_.dtype));

    if (nbytes < bytes_of_frame) {
        LOG_ERROR("Data size is less than frame size");
        return 0;
    }

    auto* data = static_cast<const uint8_t*>(data_);

    size_t bytes_written = 0;
    while (nbytes - bytes_written >= bytes_of_frame) {
        const size_t bytes_written_this_frame =
          writers_[0]->write_frame(data, bytes_of_frame);
        if (bytes_written_this_frame == 0) {
            break;
        }

        write_multiscale_frames_(data, bytes_written_this_frame);

        bytes_written += bytes_written_this_frame;
        data = (uint8_t*)data + bytes_written_this_frame;
    }

    return bytes_written;
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

bool
ZarrStream_s::create_store_()
{
    if (is_s3_acquisition(&settings_)) {
        // spin up S3 connection pool
        try {
            s3_connection_pool_ = std::make_shared<zarr::S3ConnectionPool>(
              std::thread::hardware_concurrency(),
              settings_.s3_endpoint,
              settings_.s3_access_key_id,
              settings_.s3_secret_access_key);
        } catch (const std::exception& e) {
            set_error_("Error creating S3 connection pool: " +
                       std::string(e.what()));
            return false;
        }

        // test the S3 connection
        auto conn = s3_connection_pool_->get_connection();
        if (!conn->check_connection()) {
            set_error_("Failed to connect to S3");
            return false;
        }
        s3_connection_pool_->return_connection(std::move(conn));
    } else {
        if (fs::exists(settings_.store_path)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(settings_.store_path, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           settings_.store_path + "': " + ec.message());
                return false;
            }
        }

        // create the store path
        {
            std::error_code ec;
            if (!fs::create_directories(settings_.store_path, ec)) {
                set_error_("Failed to create store path '" +
                           settings_.store_path + "': " + ec.message());
                return false;
            }
        }
    }

    return true;
}

bool
ZarrStream_s::create_writers_()
{
    writers_.clear();

    // construct Blosc compression parameters
    std::optional<zarr::BloscCompressionParams> blosc_compression_params;
    if (settings_.compressor == ZarrCompressor_Blosc1) {
        blosc_compression_params = zarr::BloscCompressionParams(
          zarr::blosc_codec_to_string(
            static_cast<ZarrCompressionCodec>(settings_.compression_codec)),
          settings_.compression_level,
          settings_.compression_shuffle);
    }

    std::optional<std::string> bucket_name;
    if (is_s3_acquisition(&settings_)) {
        bucket_name = settings_.s3_bucket_name;
    }

    zarr::ArrayWriterConfig config = {
        .dimensions = settings_.dimensions,
        .dtype = static_cast<ZarrDataType>(settings_.dtype),
        .level_of_detail = 0,
        .bucket_name = bucket_name,
        .store_path = settings_.store_path,
        .compression_params = blosc_compression_params
    };

    if (version_ == 2) {
        writers_.push_back(std::make_unique<zarr::ZarrV2ArrayWriter>(
          config, thread_pool_, s3_connection_pool_));
    } else {
        writers_.push_back(std::make_unique<zarr::ZarrV3ArrayWriter>(
          config, thread_pool_, s3_connection_pool_));
    }

    if (settings_.multiscale) {
        zarr::ArrayWriterConfig downsampled_config;

        bool do_downsample = true;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);

            if (version_ == 2) {
                writers_.push_back(std::make_unique<zarr::ZarrV2ArrayWriter>(
                  downsampled_config, thread_pool_, s3_connection_pool_));
            } else {
                writers_.push_back(std::make_unique<zarr::ZarrV3ArrayWriter>(
                  downsampled_config, thread_pool_, s3_connection_pool_));
            }
            //            scaled_frames_.emplace(level++, std::nullopt);

            config = std::move(downsampled_config);
            downsampled_config = {};
        }
    }

    return true;
}

void
ZarrStream_s::create_scaled_frames_()
{
    if (settings_.multiscale) {
        for (size_t level = 1; level < writers_.size(); ++level) {
            scaled_frames_.emplace(level, std::nullopt);
        }
    }
}

bool
ZarrStream_s::create_metadata_sinks_()
{
    zarr::SinkCreator creator(thread_pool_, s3_connection_pool_);

    try {
        if (s3_connection_pool_) {
            if (!creator.make_metadata_sinks(version_,
                                             settings_.s3_bucket_name,
                                             settings_.store_path,
                                             metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        } else {
            if (!creator.make_metadata_sinks(
                  version_, settings_.store_path, metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        }
    } catch (const std::exception& e) {
        set_error_("Error creating metadata sinks: " + std::string(e.what()));
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_base_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata["multiscales"] = make_multiscale_metadata_();

        metadata_key = ".zattrs";
    } else {
        metadata["extensions"] = nlohmann::json::array();
        metadata["metadata_encoding"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";
        metadata["metadata_key_suffix"] = ".json";
        metadata["zarr_format"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";

        metadata_key = "zarr.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();

    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing base metadata");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_group_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata = { { "zarr_format", 2 } };

        metadata_key = ".zgroup";
    } else {
        metadata["attributes"]["multiscales"] = make_multiscale_metadata_();

        metadata_key = "meta/root.group.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing group metadata");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_external_metadata_()
{
    if (settings_.external_metadata.empty()) {
        return true;
    }

    auto metadata = nlohmann::json::parse(settings_.external_metadata,
                                          nullptr, // callback
                                          false,   // allow exceptions
                                          true     // ignore comments
    );
    std::string metadata_key = "acquire.json";

    if (version_ == 3) {
        metadata_key = "meta/" + metadata_key;
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing external metadata");
        return false;
    }

    return true;
}

nlohmann::json
ZarrStream_s::make_multiscale_metadata_() const
{
    nlohmann::json multiscales;
    const auto& dimensions = settings_.dimensions;
    multiscales[0]["version"] = "0.4";

    auto& axes = multiscales[0]["axes"];
    for (auto dim = dimensions.begin(); dim != dimensions.end(); ++dim) {
        std::string type =
          dimension_type_to_string(static_cast<ZarrDimensionType>(dim->kind));

        if (dim < dimensions.end() - 2) {
            axes.push_back({ { "name", dim->name.c_str() }, { "type", type } });
        } else {
            axes.push_back({ { "name", dim->name.c_str() },
                             { "type", type },
                             { "unit", "micrometer" } });
        }
    }

    // spatial multiscale metadata
    std::vector<double> scales(dimensions.size(), 1.0);
    multiscales[0]["datasets"] = {
        {
          { "path", "0" },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        },
    };

    for (auto i = 1; i < writers_.size(); ++i) {
        scales.clear();
        scales.push_back(std::pow(2, i)); // append
        for (auto k = 0; k < dimensions.size() - 3; ++k) {
            scales.push_back(1.);
        }
        scales.push_back(std::pow(2, i)); // y
        scales.push_back(std::pow(2, i)); // x

        multiscales[0]["datasets"].push_back({
          { "path", std::to_string(i) },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        });

        // downsampling metadata
        multiscales[0]["type"] = "local_mean";
        multiscales[0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters "
              "are "
              "given here." },
            { "method", "skimage.transform.downscale_local_mean" },
            { "version", "0.21.0" },
            { "args", "[2]" },
            { "kwargs", { "cval", 0 } },
        };
    }

    return multiscales;
}

void
ZarrStream_s::write_multiscale_frames_(const uint8_t* data,
                                       size_t bytes_of_data)
{
    if (!settings_.multiscale) {
        return;
    }

    std::function<uint8_t*(const uint8_t*, size_t&, size_t&, size_t&)> scale;
    std::function<void(void*, size_t, const void*, size_t)> average2;

    auto dtype = static_cast<ZarrDataType>(settings_.dtype);
    switch (dtype) {
        case ZarrDataType_uint8:
            scale = scale_image<uint8_t>;
            average2 = average_two_frames<uint8_t>;
            break;
        case ZarrDataType_uint16:
            scale = scale_image<uint16_t>;
            average2 = average_two_frames<uint16_t>;
            break;
        case ZarrDataType_uint32:
            scale = scale_image<uint32_t>;
            average2 = average_two_frames<uint32_t>;
            break;
        case ZarrDataType_uint64:
            scale = scale_image<uint64_t>;
            average2 = average_two_frames<uint64_t>;
            break;
        case ZarrDataType_int8:
            scale = scale_image<int8_t>;
            average2 = average_two_frames<int8_t>;
            break;
        case ZarrDataType_int16:
            scale = scale_image<int16_t>;
            average2 = average_two_frames<int16_t>;
            break;
        case ZarrDataType_int32:
            scale = scale_image<int32_t>;
            average2 = average_two_frames<int32_t>;
            break;
        case ZarrDataType_int64:
            scale = scale_image<int64_t>;
            average2 = average_two_frames<int64_t>;
            break;
        case ZarrDataType_float16:
            scale = scale_image<uint16_t>;
            average2 = average_two_frames<uint16_t>;
            break;
        case ZarrDataType_float32:
            scale = scale_image<float>;
            average2 = average_two_frames<float>;
            break;
        case ZarrDataType_float64:
            scale = scale_image<double>;
            average2 = average_two_frames<double>;
            break;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(dtype));
    }

    const auto& dims = settings_.dimensions;
    size_t frame_width = dims.back().array_size_px;
    size_t frame_height = dims[dims.size() - 2].array_size_px;

    uint8_t* dst;
    for (auto i = 1; i < writers_.size(); ++i) {
        dst = scale(data, bytes_of_data, frame_width, frame_height);

        // bytes_of data is now downscaled
        // frame_width and frame_height are now the new dimensions

        if (scaled_frames_[i]) {
            average2(dst, bytes_of_data, *scaled_frames_[i], bytes_of_data);
            EXPECT(writers_[i]->write_frame(dst, bytes_of_data),
                   "Failed to write frame to writer %zu",
                   i);

            // clean up this LOD
            delete[] *scaled_frames_[i];
            scaled_frames_[i].reset();

            // set up for next iteration
            if (i + 1 < writers_.size()) {
                data = dst;
            } else {
                delete[] dst;
            }
        } else {
            scaled_frames_[i] = dst;
            break;
        }
    }
}