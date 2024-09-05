#include "zarr.stream.hh"
#include "logger.hh"

#include "zarr.h"

#include <filesystem>

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
        // todo (aliddell): implement this
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
} // namespace

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

    ZarrError ZarrStream_append(ZarrStream* stream,
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
            return ZarrError_InternalError;
        }

        return ZarrError_Success;
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

    ZarrError ZarrStream_get_dimension(const ZarrStream* stream,
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

    ZarrError Zarr_set_log_level(LogLevel level)
    {
        if (level < LogLevel_Debug || level >= LogLevelCount) {
            return ZarrError_InvalidArgument;
        }

        Logger::set_log_level(level);
        return ZarrError_Success;
    }

    LogLevel Zarr_get_log_level()
    {
        return Logger::get_log_level();
    }

    /* Error handling */

    const char* Zarr_get_error_message(ZarrError error)
    {
        switch (error) {
            case ZarrError_Success:
                return "Success";
            case ZarrError_InvalidArgument:
                return "Invalid argument";
            case ZarrError_Overflow:
                return "Overflow";
            case ZarrError_InvalidIndex:
                return "Invalid index";
            case ZarrError_NotYetImplemented:
                return "Not yet implemented";
            case ZarrError_InternalError:
                return "Internal error";
            default:
                return "Unknown error";
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
}

size_t
  ZarrStream::append(const void* data, size_t nbytes)
{
    // todo (aliddell): implement this
    return 0;
}