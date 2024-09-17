#include "macros.hh"
#include "stream.settings.hh"
#include "acquire.zarr.h"

#include <blosc.h>
#include <nlohmann/json.hpp>

#include <cstring> // memcpy, strnlen
#include <filesystem>

#define SETTINGS_GET_STRING(settings, member)                                  \
    do {                                                                       \
        if (!settings) {                                                       \
            LOG_ERROR("Null pointer: %s", #settings);                          \
            return nullptr;                                                    \
        }                                                                      \
        return settings->member.c_str();                                       \
    } while (0)

namespace fs = std::filesystem;

namespace {
const size_t zarr_dimension_min = 3;
const size_t zarr_dimension_max = 32;

[[nodiscard]]
std::string
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

[[nodiscard]]
bool
validate_s3_settings(const ZarrS3Settings* settings)
{
    size_t len;

    if (len = strnlen(settings->endpoint, settings->bytes_of_endpoint);
        len == 0) {
        LOG_ERROR("S3 endpoint is empty");
        return false;
    }

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    if (len = strnlen(settings->bucket_name, settings->bytes_of_bucket_name);
        len < 4 || len > 64) {
        LOG_ERROR("Invalid length for S3 bucket name: %zu. Must be between 3 "
                  "and 63 characters",
                  len);
        return false;
    }

    if (len =
          strnlen(settings->access_key_id, settings->bytes_of_access_key_id);
        len == 0) {
        LOG_ERROR("S3 access key ID is empty");
        return false;
    }

    if (len = strnlen(settings->secret_access_key,
                      settings->bytes_of_secret_access_key);
        len == 0) {
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
validate_compression_settings(const ZarrCompressionSettings* settings)
{
    if (settings->compressor >= ZarrCompressorCount) {
        LOG_ERROR("Invalid compressor: %d", settings->compressor);
        return false;
    }

    if (settings->codec >= ZarrCompressionCodecCount) {
        LOG_ERROR("Invalid compression codec: %d", settings->codec);
        return false;
    }

    // if compressing, we require a compression codec
    if (settings->compressor != ZarrCompressor_None &&
        settings->codec == ZarrCompressionCodec_None) {
        LOG_ERROR("Compression codec must be set when using a compressor");
        return false;
    }

    if (settings->level > 9) {
        LOG_ERROR("Invalid compression level: %d. Must be between 0 and 9",
                  settings->level);
        return false;
    }

    if (settings->shuffle != BLOSC_NOSHUFFLE &&
        settings->shuffle != BLOSC_SHUFFLE &&
        settings->shuffle != BLOSC_BITSHUFFLE) {
        LOG_ERROR("Invalid shuffle: %d. Must be %d (no shuffle), %d (byte "
                  "shuffle), or %d (bit shuffle)",
                  settings->shuffle,
                  BLOSC_NOSHUFFLE,
                  BLOSC_SHUFFLE,
                  BLOSC_BITSHUFFLE);
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_dimension(const ZarrDimensionProperties* dimension)
{
    std::string trimmed = trim(dimension->name, dimension->bytes_of_name - 1);
    if (trimmed.empty()) {
        LOG_ERROR("Invalid name. Must not be empty");
        return false;
    }

    if (dimension->type >= ZarrDimensionTypeCount) {
        LOG_ERROR("Invalid dimension type: %d", dimension->type);
        return false;
    }

    if (dimension->chunk_size_px == 0) {
        LOG_ERROR("Invalid chunk size: %zu", dimension->chunk_size_px);
        return false;
    }

    return true;
}
} // namespace

ZarrStreamSettings_s::ZarrStreamSettings_s()
  : store_path()
  , s3_endpoint()
  , s3_bucket_name()
  , s3_access_key_id()
  , s3_secret_access_key()
  , custom_metadata("{}")
  , dtype(ZarrDataType_uint8)
  , compressor(ZarrCompressor_None)
  , compression_codec(ZarrCompressionCodec_None)
  , compression_level(0)
  , compression_shuffle(BLOSC_NOSHUFFLE)
  , dimensions()
  , multiscale(false)
{
}

/* Lifecycle */
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

    *copy = *settings;

    return copy;
}

/* Setters */
ZarrStatus
ZarrStreamSettings_set_store(ZarrStreamSettings* settings,
                             const char* store_path,
                             size_t bytes_of_store_path,
                             const ZarrS3Settings* s3_settings)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(store_path, "Null pointer: store_path");

    bytes_of_store_path = strnlen(store_path, bytes_of_store_path);
    EXPECT_VALID_ARGUMENT(bytes_of_store_path > 1,
                          "Invalid store path. Must not be empty");

    std::string_view store_path_sv(store_path, bytes_of_store_path);
    if (store_path_sv.empty()) {
        LOG_ERROR("Invalid store path. Must not be empty");
        return ZarrStatus_InvalidArgument;
    }

    if (nullptr != s3_settings) {
        if (!validate_s3_settings(s3_settings)) {
            return ZarrStatus_InvalidArgument;
        }
    } else if (!validate_filesystem_store_path(store_path)) {
        return ZarrStatus_InvalidArgument;
    }

    if (nullptr != s3_settings) {
        settings->s3_endpoint = s3_settings->endpoint;
        settings->s3_bucket_name = s3_settings->bucket_name;
        settings->s3_access_key_id = s3_settings->access_key_id;
        settings->s3_secret_access_key = s3_settings->secret_access_key;
    }

    settings->store_path = store_path;

    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_set_compression(
  ZarrStreamSettings* settings,
  const ZarrCompressionSettings* compression_settings)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(compression_settings,
                          "Null pointer: compression_settings");

    if (!validate_compression_settings(compression_settings)) {
        return ZarrStatus_InvalidArgument;
    }

    settings->compressor = compression_settings->compressor;
    settings->compression_codec = compression_settings->codec;
    settings->compression_level = compression_settings->level;
    settings->compression_shuffle = compression_settings->shuffle;

    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_set_custom_metadata(ZarrStreamSettings* settings,
                                       const char* external_metadata,
                                       size_t bytes_of_external_metadata)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return ZarrStatus_InvalidArgument;
    }

    if (!external_metadata) {
        LOG_ERROR("Null pointer: custom_metadata");
        return ZarrStatus_InvalidArgument;
    }

    if (bytes_of_external_metadata == 0) {
        LOG_ERROR("Invalid length: %zu. Must be greater than 0",
                  bytes_of_external_metadata);
        return ZarrStatus_InvalidArgument;
    }

    size_t nbytes = strnlen(external_metadata, bytes_of_external_metadata);
    if (nbytes < 2) {
        settings->custom_metadata = "{}";
        return ZarrStatus_Success;
    }

    auto val = nlohmann::json::parse(external_metadata,
                                     external_metadata + nbytes,
                                     nullptr, // callback
                                     false,   // allow exceptions
                                     true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: %s", external_metadata);
        return ZarrStatus_InvalidArgument;
    }
    settings->custom_metadata = val.dump();

    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_set_data_type(ZarrStreamSettings* settings,
                                 ZarrDataType data_type)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(
      data_type < ZarrDataTypeCount, "Invalid pixel type: %d", data_type);

    settings->dtype = data_type;
    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_reserve_dimensions(ZarrStreamSettings* settings,
                                      size_t count)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(count >= zarr_dimension_min &&
                            count <= zarr_dimension_max,
                          "Invalid count: %zu. Count must be between %d and %d",
                          count,
                          zarr_dimension_min,
                          zarr_dimension_max);

    settings->dimensions.resize(count);
    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
                                 size_t index,
                                 const ZarrDimensionProperties* dimension)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
    EXPECT_VALID_ARGUMENT(dimension, "Null pointer: dimension");
    EXPECT_VALID_INDEX(index < settings->dimensions.size(),
                       "Invalid index: %zu. Must be less than %zu",
                       index,
                       settings->dimensions.size());

    if (!validate_dimension(dimension)) {
        return ZarrStatus_InvalidArgument;
    }

    struct ZarrDimension_s& dim = settings->dimensions[index];

    dim.name = trim(dimension->name, dimension->bytes_of_name - 1);
    dim.type = dimension->type;
    dim.array_size_px = dimension->array_size_px;
    dim.chunk_size_px = dimension->chunk_size_px;
    dim.shard_size_chunks = dimension->shard_size_chunks;

    return ZarrStatus_Success;
}

ZarrStatus
ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
                                  uint8_t multiscale)
{
    EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");

    settings->multiscale = multiscale > 0;
    return ZarrStatus_Success;
}

/* Getters */
const char*
ZarrStreamSettings_get_store_path(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, store_path);
}

ZarrS3Settings
ZarrStreamSettings_get_s3_settings(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning empty S3 settings.");
        return {};
    }

    ZarrS3Settings s3_settings = {
        settings->s3_endpoint.c_str(),
        settings->s3_endpoint.length() + 1,
        settings->s3_bucket_name.c_str(),
        settings->s3_bucket_name.length() + 1,
        settings->s3_access_key_id.c_str(),
        settings->s3_access_key_id.length() + 1,
        settings->s3_secret_access_key.c_str(),
        settings->s3_secret_access_key.length() + 1,
    };
    return s3_settings;
}

const char*
ZarrStreamSettings_get_custom_metadata(const ZarrStreamSettings* settings)
{
    SETTINGS_GET_STRING(settings, custom_metadata);
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

ZarrCompressionSettings
ZarrStreamSettings_get_compression(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning empty compression.");
        return {};
    }

    ZarrCompressionSettings compression = {
        .compressor = settings->compressor,
        .codec = static_cast<ZarrCompressionCodec>(settings->compression_codec),
        .level = settings->compression_level,
        .shuffle = settings->compression_shuffle,
    };
    return compression;
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

ZarrDimensionProperties
ZarrStreamSettings_get_dimension(const ZarrStreamSettings* settings,
                                 size_t index)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning empty dimension.");
        return {};
    }

    if (index >= settings->dimensions.size()) {
        LOG_ERROR("Invalid index: %zu. Must be less than %zu",
                  index,
                  settings->dimensions.size());
        return {};
    }

    const auto& dim = settings->dimensions[index];

    ZarrDimensionProperties dimension = {
        .name = dim.name.c_str(),
        .bytes_of_name = dim.name.size() + 1,
        .type = dim.type,
        .array_size_px = dim.array_size_px,
        .chunk_size_px = dim.chunk_size_px,
        .shard_size_chunks = dim.shard_size_chunks,
    };

    return dimension;
}

bool
ZarrStreamSettings_get_multiscale(const ZarrStreamSettings* settings)
{
    if (!settings) {
        LOG_WARNING("Null pointer: settings. Returning false.");
        return false;
    }
    return settings->multiscale;
}
