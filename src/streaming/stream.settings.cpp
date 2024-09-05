#include "stream.settings.hh"
#include "zarr.h"
#include "logger.hh"

#include <blosc.h>
#include <nlohmann/json.hpp>

#include <cstring> // memcpy

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
        default:
            return "(unknown)";
    }
}

const char*
compression_codec_to_string(ZarrCompressionCodec codec)
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

inline std::string
trim(const char* s, size_t bytes_of_s)
{
    // trim left
    std::string trimmed(s, bytes_of_s);
    trimmed.erase(trimmed.begin(),
                  std::find_if(trimmed.begin(), trimmed.end(), [](char c) {
                      return !std::isspace(c);
                  }));

    // trim right
    trimmed.erase(std::find_if(trimmed.rbegin(),
                               trimmed.rend(),
                               [](char c) { return !std::isspace(c); })
                    .base(),
                  trimmed.end());

    return trimmed;
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

ZarrStreamSettings*
ZarrStreamSettings_copy(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return nullptr;
    }

    ZarrStreamSettings* copy = ZarrStreamSettings_create();
    if (!copy) {
        LOG_ERROR("Failed to allocate memory for copy");
        return nullptr;
    }

    copy->store_path = settings->store_path;
    copy->s3_endpoint = settings->s3_endpoint;
    copy->s3_bucket_name = settings->s3_bucket_name;
    copy->s3_access_key_id = settings->s3_access_key_id;
    copy->s3_secret_access_key = settings->s3_secret_access_key;
    copy->external_metadata = settings->external_metadata;
    copy->dtype = settings->dtype;
    copy->compressor = settings->compressor;
    copy->compression_codec = settings->compression_codec;
    copy->compression_level = settings->compression_level;
    copy->compression_shuffle = settings->compression_shuffle;
    copy->dimensions = settings->dimensions;
    copy->multiscale = settings->multiscale;

    return copy;
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
ZarrStreamSettings_set_external_metadata(ZarrStreamSettings* settings,
                                         const char* external_metadata,
                                         size_t bytes_of_external_metadata)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return ZarrError_InvalidArgument;
    }

    if (!external_metadata) {
        LOG_ERROR("Null pointer: external_metadata");
        return ZarrError_InvalidArgument;
    }

    if (bytes_of_external_metadata == 0) {
        LOG_ERROR("Invalid length: %zu. Must be greater than 0",
                  bytes_of_external_metadata);
        return ZarrError_InvalidArgument;
    }

    const size_t nbytes =
      strnlen(external_metadata, bytes_of_external_metadata);
    if (nbytes < 2) {
        return ZarrError_Success;
    }

    auto val =  nlohmann::json::parse(external_metadata,
                                        external_metadata + nbytes,
                                        nullptr, // callback
                                        false,    // allow exceptions
                                        true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: %s", external_metadata);
        return ZarrError_InvalidArgument;
    }
    settings->external_metadata = val.dump();

    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_data_type(ZarrStreamSettings* settings,
                                 ZarrDataType data_type)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(
      data_type < ZarrDataTypeCount, "Invalid pixel type: %d", data_type);

    settings->dtype = data_type;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_compressor(ZarrStreamSettings* settings,
                                  ZarrCompressor compressor)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(
      compressor < ZarrCompressorCount, "Invalid compressor: %d", compressor);

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
                          compression_codec_to_string(codec));

    settings->compression_codec = codec;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_compression_level(ZarrStreamSettings* settings,
                                         uint8_t level)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(level >= 0 && level <= 9,
                          "Invalid level: %d. Must be between 0 (no "
                          "compression) and 9 (maximum compression).",
                          level);

    settings->compression_level = level;

    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_compression_shuffle(ZarrStreamSettings* settings,
                                           uint8_t shuffle)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(shuffle == BLOSC_NOSHUFFLE ||
                            shuffle == BLOSC_SHUFFLE ||
                            shuffle == BLOSC_BITSHUFFLE,
                          "Invalid shuffle: %d. Must be %d (no shuffle), %d "
                          "(byte shuffle), or %d (bit shuffle)",
                          shuffle,
                          BLOSC_NOSHUFFLE,
                          BLOSC_SHUFFLE,
                          BLOSC_BITSHUFFLE);

    settings->compression_shuffle = shuffle;
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
                                 uint32_t array_size_px,
                                 uint32_t chunk_size_px,
                                 uint32_t shard_size_chunks)
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

    std::string dim_name = trim(name, bytes_of_name);
    if (dim_name.empty()) {
        LOG_ERROR("Invalid name. Must not be empty");
        return ZarrError_InvalidArgument;
    }

    struct ZarrDimension_s* dim = &settings->dimensions[index];

    dim->name = dim_name;
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
ZarrStreamSettings_get_store_path(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, store_path);
}

const char*
ZarrStreamSettings_get_s3_endpoint(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_endpoint);
}

const char*
ZarrStreamSettings_get_s3_bucket_name(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_bucket_name);
}

const char*
ZarrStreamSettings_get_s3_access_key_id(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_access_key_id);
}

const char*
ZarrStreamSettings_get_s3_secret_access_key(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, s3_secret_access_key);
}

const char*
ZarrStreamSettings_get_external_metadata(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, external_metadata);
}

ZarrDataType
ZarrStreamSettings_get_data_type(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning DataType_uint8.");
        return ZarrDataType_uint8;
    }
    return static_cast<ZarrDataType>(settings->dtype);
}

ZarrCompressor
ZarrStreamSettings_get_compressor(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning ZarrCompressor_None.");
        return ZarrCompressor_None;
    }
    return static_cast<ZarrCompressor>(settings->compressor);
}

ZarrCompressionCodec
ZarrStreamSettings_get_compression_codec(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING(
          "Null pointer: settings. Returning ZarrCompressionCodec_None.");
        return ZarrCompressionCodec_None;
    }
    return static_cast<ZarrCompressionCodec>(settings->compression_codec);
}

uint8_t
ZarrStreamSettings_get_compression_level(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning 0.");
        return 0;
    }
    return settings->compression_level;
}

uint8_t
ZarrStreamSettings_get_compression_shuffle(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning 0.");
        return 0;
    }
    return settings->compression_shuffle;
}

size_t
ZarrStreamSettings_get_dimension_count(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning 0.");
        return 0;
    }
    return settings->dimensions.size();
}

ZarrError
ZarrStreamSettings_get_dimension(const ZarrStreamSettings* settings,
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

    const struct ZarrDimension_s* dim = &settings->dimensions[index];

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
ZarrStreamSettings_get_multiscale(const ZarrStreamSettings* settings)
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
