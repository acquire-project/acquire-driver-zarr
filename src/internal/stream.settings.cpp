#include <cstring> // memcpy

#include "stream.settings.hh"
#include "zarr.h"
#include "logger.hh"

#define EXPECT_VALID_ARGUMENT(e, ...)                                          \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrError_InvalidArgument;                                  \
        }                                                                      \
    } while (0)

#define ZARR_DIMENSION_MIN 3
#define ZARR_DIMENSION_MAX 32

#define SETTINGS_SET_STRING(settings, member, bytes_of_member)                 \
    do {                                                                       \
        if (!(settings)) {                                                     \
            LOG_ERROR("Null pointer: %s", #settings);                          \
            return ZarrError_InvalidArgument;                                  \
        }                                                                      \
        if (!(member)) {                                                       \
            LOG_ERROR("Null pointer: %s", #member);                            \
            return ZarrError_InvalidArgument;                                  \
        }                                                                      \
        size_t nbytes = strnlen(member, bytes_of_member);                      \
        settings->member = { member, nbytes };                                 \
        return ZarrError_Success;                                              \
    } while (0)

#define SETTINGS_GET_STRING(settings, member)                                  \
    do {                                                                       \
        if (!settings) {                                                       \
            LOG_ERROR("Null pointer: %s", #settings);                          \
            return nullptr;                                                    \
        }                                                                      \
        return settings->member.c_str();                                       \
    } while (0)

namespace {
const char*
compressor_to_string(ZarrCompressor compressor)
{
    switch (compressor) {
        case ZarrCompressor_None:
            return "none";
        case ZarrCompressor_Blosc1:
            return "blosc1";
        case ZarrCompressor_Blosc2:
            return "blosc2";
        case ZarrCompressor_Zstd:
            return "zstd";
        default:
            return "(unknown)";
    }
}

const char*
codec_to_string(ZarrCompressionCodec codec)
{
    switch (codec) {
        case ZarrCompressionCodec_None:
            return "none";
        case ZarrCompressionCodec_BloscLZ4:
            return "blosc-lz4";
        case ZarrCompressionCodec_BloscZstd:
            return "blosc-zstd";
        default:
            return "(unknown)";
    }
}

const char*
dimension_type_to_string(ZarrDimensionType kind)
{
    switch (kind) {
        case ZarrDimensionType_Space:
            return "space";
        case ZarrDimensionType_Time:
            return "time";
        case ZarrDimensionType_Channel:
            return "channel";
        case ZarrDimensionType_Other:
            return "other";
        default:
            return "(unknown)";
    }
}
} // namespace

/* Create and destroy */
ZarrStreamSettings*
ZarrStreamSettings_create()
{
    try {
        return new ZarrStreamSettings();
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

void
ZarrStreamSettings_destroy(ZarrStreamSettings* settings)
{
    delete settings;
}

/* Setters */
ZarrError
ZarrStreamSettings_set_store_path(ZarrStreamSettings* settings,
                                  const char* store_path,
                                  size_t bytes_of_store_path)
{
    SETTINGS_SET_STRING(settings, store_path, bytes_of_store_path);
}

ZarrError
ZarrStreamSettings_set_s3_endpoint(ZarrStreamSettings* settings,
                                   const char* s3_endpoint,
                                   size_t bytes_of_s3_endpoint)
{
    SETTINGS_SET_STRING(settings, s3_endpoint, bytes_of_s3_endpoint);
}

ZarrError
ZarrStreamSettings_set_s3_bucket_name(ZarrStreamSettings* settings,
                                      const char* s3_bucket_name,
                                      size_t bytes_of_s3_bucket_name)
{
    SETTINGS_SET_STRING(settings, s3_bucket_name, bytes_of_s3_bucket_name);
}

ZarrError
ZarrStreamSettings_set_s3_access_key_id(ZarrStreamSettings* settings,
                                        const char* s3_access_key_id,
                                        size_t bytes_of_s3_access_key_id)
{
    SETTINGS_SET_STRING(settings, s3_access_key_id, bytes_of_s3_access_key_id);
}

ZarrError
ZarrStreamSettings_set_s3_secret_access_key(
  ZarrStreamSettings* settings,
  const char* s3_secret_access_key,
  size_t bytes_of_s3_secret_access_key)
{
    SETTINGS_SET_STRING(
      settings, s3_secret_access_key, bytes_of_s3_secret_access_key);
}

ZarrError
ZarrStreamSettings_set_compressor(ZarrStreamSettings* settings,
                                  ZarrCompressor compressor)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(
      compressor < ZarrCompressorCount, "Invalid compressor: %d", compressor);

    if (compressor >= ZarrCompressor_Blosc2) {
        LOG_ERROR("Compressor not yet implemented: %s",
                  compressor_to_string(compressor));
        return ZarrError_NotYetImplemented;
    }

    settings->compressor = compressor;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_compression_codec(ZarrStreamSettings* settings,
                                         ZarrCompressionCodec codec)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(codec < ZarrCompressionCodecCount,
                          "Invalid codec: %s",
                          codec_to_string(codec));

    settings->codec = codec;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_reserve_dimensions(ZarrStreamSettings* settings,
                                      size_t count)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(count >= ZARR_DIMENSION_MIN &&
                            count <= ZARR_DIMENSION_MAX,
                          "Invalid count: %zu. Count must be between %d and %d",
                          count,
                          ZARR_DIMENSION_MIN,
                          ZARR_DIMENSION_MAX);

    settings->dimensions.resize(count);
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
                                 size_t index,
                                 const char* name,
                                 size_t bytes_of_name,
                                 ZarrDimensionType kind,
                                 size_t array_size_px,
                                 size_t chunk_size_px,
                                 size_t shard_size_chunks)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(name, "Null pointer: name");
    EXPECT_VALID_ARGUMENT(
      bytes_of_name > 0, "Invalid name length: %zu", bytes_of_name);
    EXPECT_VALID_ARGUMENT(strnlen(name, bytes_of_name) > 0,
                          "Invalid name. Must not be empty");
    EXPECT_VALID_ARGUMENT(
      kind < ZarrDimensionTypeCount, "Invalid dimension type: %d", kind);
    EXPECT_VALID_ARGUMENT(
      chunk_size_px > 0, "Invalid chunk size: %zu", chunk_size_px);

    // Check that the index is within bounds
    if (index >= settings->dimensions.size()) {
        LOG_ERROR("Invalid index: %zu. Must be less than %zu",
                  index,
                  settings->dimensions.size());
        return ZarrError_InvalidIndex;
    }

    struct ZarrDimension_s* dim = &settings->dimensions[index];

    dim->name = { name, strnlen(name, bytes_of_name) };
    dim->kind = kind;
    dim->array_size_px = array_size_px;
    dim->chunk_size_px = chunk_size_px;
    dim->shard_size_chunks = shard_size_chunks;

    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
                                  uint8_t multiscale)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");

    settings->multiscale = multiscale > 0;
    return ZarrError_Success;
}

/* Getters */
const char*
ZarrStreamSettings_get_store_path(ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, store_path);
}

const char*
ZarrStreamSettings_get_s3_endpoint(ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_endpoint);
}

const char*
ZarrStreamSettings_get_s3_bucket_name(ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_bucket_name);
}

const char*
ZarrStreamSettings_get_s3_access_key_id(ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_access_key_id);
}

const char*
ZarrStreamSettings_get_s3_secret_access_key(ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_secret_access_key);
}

ZarrCompressor
ZarrStreamSettings_get_compressor(ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning ZarrCompressor_None.");
        return ZarrCompressor_None;
    }
    return static_cast<ZarrCompressor>(settings->compressor);
}

ZarrCompressionCodec
ZarrStreamSettings_get_compression_codec(ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING(
          "Null pointer: settings. Returning ZarrCompressionCodec_None.");
        return ZarrCompressionCodec_None;
    }
    return static_cast<ZarrCompressionCodec>(settings->codec);
}

size_t
ZarrStreamSettings_get_dimension_count(ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning 0.");
        return 0;
    }
    return settings->dimensions.size();
}

ZarrError
ZarrStreamSettings_get_dimension(ZarrStreamSettings* settings,
                                 size_t index,
                                 char* name,
                                 size_t bytes_of_name,
                                 ZarrDimensionType* kind,
                                 size_t* array_size_px,
                                 size_t* chunk_size_px,
                                 size_t* shard_size_chunks)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(name, "Null pointer: name");
    EXPECT_VALID_ARGUMENT(kind, "Null pointer: kind");
    EXPECT_VALID_ARGUMENT(array_size_px, "Null pointer: array_size_px");
    EXPECT_VALID_ARGUMENT(chunk_size_px, "Null pointer: chunk_size_px");
    EXPECT_VALID_ARGUMENT(shard_size_chunks, "Null pointer: shard_size_chunks");

    if (index >= settings->dimensions.size()) {
        LOG_ERROR("Invalid index: %zu. Must be less than %zu",
                  index,
                  settings->dimensions.size());
        return ZarrError_InvalidIndex;
    }

    struct ZarrDimension_s* dim = &settings->dimensions[index];

    if (bytes_of_name < dim->name.length() + 1) {
        LOG_ERROR("Insufficient buffer size: %zu. Need at least %zu",
                  bytes_of_name,
                  dim->name.length() + 1);
        return ZarrError_Overflow;
    }

    memcpy(name, dim->name.c_str(), dim->name.length() + 1);
    *kind = static_cast<ZarrDimensionType>(dim->kind);
    *array_size_px = dim->array_size_px;
    *chunk_size_px = dim->chunk_size_px;
    *shard_size_chunks = dim->shard_size_chunks;

    return ZarrError_Success;
}

uint8_t
ZarrStreamSettings_get_multiscale(ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning 0.");
        return 0;
    }
    return static_cast<uint8_t>(settings->multiscale);
}

/* Internal functions */

bool
validate_dimension(const struct ZarrDimension_s& dimension)
{
    return !dimension.name.empty() && dimension.kind < ZarrDimensionTypeCount &&
           dimension.chunk_size_px > 0;
}
