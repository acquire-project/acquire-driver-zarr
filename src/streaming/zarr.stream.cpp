#include "zarr.stream.hh"
#include "logger.hh"
#include "zarr.types.h"

#include <blosc.h>

#include <filesystem>

#define EXPECT_VALID_ARGUMENT(e, ...)                                          \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrStatus_InvalidArgument;                                 \
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
is_compressed_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return settings->compressor != ZarrCompressor_None;
}

[[nodiscard]]
bool
validate_s3_settings(const struct ZarrStreamSettings_s* settings)
{
    if (settings->s3_endpoint.empty()) {
        LOG_ERROR("S3 endpoint is empty");
        return false;
    }
    if (settings->s3_bucket_name.length() < 3 ||
        settings->s3_bucket_name.length() > 63) {
        LOG_ERROR("Invalid length for S3 bucket name: %zu. Must be between 3 "
                  "and 63 characters",
                  settings->s3_bucket_name.length());
        return false;
    }
    if (settings->s3_access_key_id.empty()) {
        LOG_ERROR("S3 access key ID is empty");
        return false;
    }
    if (settings->s3_secret_access_key.empty()) {
        LOG_ERROR("S3 secret access key is empty");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_filesystem_store_path(std::string_view data_root)
{
    fs::path path(data_root);
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

    return true;
}

[[nodiscard]]
bool
validate_compression_settings(const ZarrStreamSettings_s* settings)
{
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

    if (settings->compression_level > 9) {
        LOG_ERROR("Invalid compression level: %d. Must be between 0 and 9",
                  settings->compression_level);
        return false;
    }

    if (settings->compression_shuffle != BLOSC_NOSHUFFLE &&
        settings->compression_shuffle != BLOSC_SHUFFLE &&
        settings->compression_shuffle != BLOSC_BITSHUFFLE) {
        LOG_ERROR("Invalid shuffle: %d. Must be %d (no shuffle), %d (byte "
                  "shuffle), or %d (bit shuffle)",
                  settings->compression_shuffle,
                  BLOSC_NOSHUFFLE,
                  BLOSC_SHUFFLE,
                  BLOSC_BITSHUFFLE);
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_dimension(const struct ZarrDimension_s& dimension,
                   ZarrVersion version,
                   bool is_append)
{
    if (dimension.name.empty()) {
        LOG_ERROR("Invalid name. Must not be empty");
        return false;
    }

    if (dimension.kind >= ZarrDimensionTypeCount) {
        LOG_ERROR("Invalid dimension type: %d", dimension.kind);
        return false;
    }

    if (!is_append && dimension.array_size_px == 0) {
        LOG_ERROR("Array size must be nonzero");
        return false;
    }

    if (dimension.chunk_size_px == 0) {
        LOG_ERROR("Chunk size must be nonzero");
        return false;
    }

    if (version == ZarrVersion_3 && dimension.shard_size_chunks == 0) {
        LOG_ERROR("Shard size must be nonzero");
        return false;
    }

    return true;
}

[[nodiscard]]
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

    std::string_view store_path(settings->store_path);

    // we require the store path (root of the dataset) to be nonempty
    if (store_path.empty()) {
        LOG_ERROR("Store path is empty");
        return false;
    }

    if (is_s3_acquisition(settings) && !validate_s3_settings(settings)) {
        return false;
    } else if (!is_s3_acquisition(settings) &&
               !validate_filesystem_store_path(store_path)) {
        return false;
    }

    if (settings->dtype >= ZarrDataTypeCount) {
        LOG_ERROR("Invalid data type: %d", settings->dtype);
        return false;
    }

    if (is_compressed_acquisition(settings) &&
        !validate_compression_settings(settings)) {
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < settings->dimensions.size(); ++i) {
        if (!validate_dimension(settings->dimensions[i], version, i == 0)) {
            return false;
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
        ZarrStream_s* stream = nullptr;

        try {
            stream = new ZarrStream(settings, version);
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for Zarr stream");
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating Zarr stream: %s", e.what());
        }

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

    ZarrVersion ZarrStream_get_version(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning ZarrVersion_2");
            return ZarrVersion_2;
        }

        return stream->version();
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

    ZarrStatus Zarr_set_log_level(ZarrLogLevel level)
    {
        if (level < ZarrLogLevel_Debug || level >= ZarrLogLevelCount) {
            return ZarrStatus_InvalidArgument;
        }

        Logger::set_log_level(level);
        return ZarrStatus_Success;
    }

    ZarrLogLevel Zarr_get_log_level()
    {
        return Logger::get_log_level();
    }

    /* Error handling */

    const char* Zarr_get_error_message(ZarrStatus error)
    {
        switch (error) {
            case ZarrStatus_Success:
                return "Success";
            case ZarrStatus_InvalidArgument:
                return "Invalid argument";
            case ZarrStatus_Overflow:
                return "Buffer overflow";
            case ZarrStatus_InvalidIndex:
                return "Invalid index";
            case ZarrStatus_NotYetImplemented:
                return "Not yet implemented";
            case ZarrStatus_InternalError:
                return "Internal error";
            case ZarrStatus_OutOfMemory:
                return "Out of memory";
            case ZarrStatus_IOError:
                return "I/O error";
            case ZarrStatus_CompressionError:
                return "Compression error";
            case ZarrStatus_InvalidSettings:
                return "Invalid settings";
            default:
                return "Unknown error";
        }
    }
}

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings,
                         ZarrVersion version)
  : settings_(*settings)
  , version_(version)
  , error_()
{
}

size_t
ZarrStream::append(const void* data, size_t nbytes)
{
    // todo (aliddell): implement this
    return 0;
}