#include <filesystem>
#include <string_view>

#include "zarr.stream.hh"
#include "zarr.h"
#include "logger.hh"

#define EXPECT_VALID_ARGUMENT(e, ...)                                          \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrError_InvalidArgument;                                  \
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
validate_settings(struct ZarrStreamSettings_s* settings, ZarrVersion version)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return false;
    }
    if (!settings || version < ZarrVersion_2 || version >= ZarrVersionCount) {
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < settings->dimensions.size(); ++i) {
        if (!validate_dimension(settings->dimensions[i])) {
            return false;
        }

        if (i > 0 && settings->dimensions[i].array_size_px == 0) {
            return false;
        }
    }

    // if version 3, we require shard size to be positive
    if (version == ZarrVersion_3) {
        for (const auto& dim : settings->dimensions) {
            if (dim.shard_size_chunks == 0) {
                return false;
            }
        }
    }

    std::string_view store_path(settings->store_path);
    std::string_view s3_endpoint(settings->s3_endpoint);
    std::string_view s3_bucket_name(settings->s3_bucket_name);
    std::string_view s3_access_key_id(settings->s3_access_key_id);
    std::string_view s3_secret_access_key(settings->s3_secret_access_key);

    // if the store path is empty, we require all S3 settings
    if (store_path.empty()) {
        if (s3_endpoint.empty() || s3_bucket_name.empty() ||
            s3_access_key_id.empty() || s3_secret_access_key.empty())
            return false;

        // check that the S3 endpoint is a valid URL
        if (s3_endpoint.find("http://") != 0 &&
            s3_endpoint.find("https://") != 0)
            return false;
    } else {
        // check that the store path either exists and is a valid directory
        // or that it can be created
        fs::path path(store_path);
        fs::path parent_path = path.parent_path();
        if (parent_path.empty()) {
            parent_path = ".";
        }

        // parent path must exist and be a directory
        if (!fs::exists(parent_path) || !fs::is_directory(parent_path)) {
            return false;
        }

        // parent path must be writable
        const auto perms = fs::status(parent_path).permissions();
        const bool is_writable =
          (perms & (fs::perms::owner_write | fs::perms::group_write |
                    fs::perms::others_write)) != fs::perms::none;

        if (!is_writable) {
            return false;
        }
    }

    return true;
}
} // namespace

ZarrStream*
ZarrStream_create(struct ZarrStreamSettings_s* settings, ZarrVersion version)
{
    if (!validate_settings(settings, version)) {
        return nullptr;
    }

    // initialize the stream
    ZarrStream_s* stream;

    try {
        stream = new ZarrStream(settings, version);
    } catch (const std::bad_alloc&) {
        return nullptr;
    } catch (const std::exception& e) {
        LOG_ERROR("Error creating Zarr stream: %s", e.what());
        return nullptr;
    }
    ZarrStreamSettings_destroy(settings);

    return stream;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    delete stream;
}

/* Getters */

ZarrVersion
ZarrStream_get_version(ZarrStream* stream)
{
    if (!stream) {
        LOG_WARNING("Null pointer: stream. Returning ZarrVersion_2");
        return ZarrVersion_2;
    }
    return static_cast<ZarrVersion>(stream->version());
}

const char*
ZarrStream_get_store_path(ZarrStream* stream)
{
    STREAM_GET_STRING(stream, store_path);
}

const char*
ZarrStream_get_s3_endpoint(ZarrStream* stream)
{
    STREAM_GET_STRING(stream, s3_endpoint);
}

const char*
ZarrStream_get_s3_bucket_name(ZarrStream* stream)
{
    STREAM_GET_STRING(stream, s3_bucket_name);
}

const char*
ZarrStream_get_s3_access_key_id(ZarrStream* stream)
{
    STREAM_GET_STRING(stream, s3_access_key_id);
}

const char*
ZarrStream_get_s3_secret_access_key(ZarrStream* stream)
{
    STREAM_GET_STRING(stream, s3_secret_access_key);
}

ZarrCompressor
ZarrStream_get_compressor(ZarrStream* stream)
{
    if (!stream) {
        LOG_WARNING("Null pointer: stream. Returning ZarrCompressor_None");
        return ZarrCompressor_None;
    }
    return ZarrStreamSettings_get_compressor(&stream->settings());
}

ZarrCompressionCodec
ZarrStream_get_compression_codec(ZarrStream* stream)
{
    if (!stream) {
        LOG_WARNING(
          "Null pointer: stream. Returning ZarrCompressionCodec_None");
        return ZarrCompressionCodec_None;
    }
    return ZarrStreamSettings_get_compression_codec(&stream->settings());
}

size_t
ZarrStream_get_dimension_count(ZarrStream* stream)
{
    if (!stream) {
        LOG_WARNING("Null pointer: stream. Returning 0");
        return 0;
    }
    return ZarrStreamSettings_get_dimension_count(&stream->settings());
}

ZarrError
ZarrStream_get_dimension(ZarrStream* stream,
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

uint8_t
ZarrStream_get_multiscale(ZarrStream* stream)
{
    if (!stream) {
        LOG_WARNING("Null pointer: stream. Returning 0");
        return 0;
    }
    return ZarrStreamSettings_get_multiscale(&stream->settings());
}

/* Logging */

ZarrError
Zarr_set_log_level(LogLevel level)
{
    if (level < LogLevel_Debug || level >= LogLevelCount) {
        return ZarrError_InvalidArgument;
    }

    Logger::set_log_level(level);
    return ZarrError_Success;
}

LogLevel
Zarr_get_log_level()
{
    return Logger::get_log_level();
}

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings, size_t version)
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
    if (!create_store_()) {
        throw std::runtime_error("Error creating Zarr stream: " + error_);
    }

    // allocate writers
}

ZarrStream_s::~ZarrStream_s()
{
    thread_pool_->await_stop();
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

bool
ZarrStream_s::create_store_()
{
    // if the store path is empty, we're using S3
    if (settings_.store_path.empty()) {
        create_s3_connection_pool_();
    } else {
        create_filesystem_store_();
    }

    // Error should be set if the store creation failed
    return error_.empty();
}

void
ZarrStream_s::create_filesystem_store_()
{
    if (fs::exists(settings_.store_path)) {
        // remove everything inside the store path
        std::error_code ec;
        fs::remove_all(settings_.store_path, ec);

        if (ec) {
            set_error_("Failed to remove existing store path '" +
                       settings_.store_path + "': " + ec.message());
            return;
        }
    }

    // create the store path
    fs::create_directories(settings_.store_path);
}

void
ZarrStream_s::create_s3_connection_pool_()
{
    // spin up S3 connection pool
    s3_connection_pool_ = std::make_shared<zarr::S3ConnectionPool>(
      std::thread::hardware_concurrency(),
      settings_.s3_endpoint,
      settings_.s3_access_key_id,
      settings_.s3_secret_access_key);
}