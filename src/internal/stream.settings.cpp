#include <cstring> // memcpy

#include "stream.settings.hh"
#include "zarr.h"

#define ZARR_DIMENSION_MIN 3
#define ZARR_DIMENSION_MAX 32

#define ZARR_STREAM_SET_STRING(stream, member, bytes_of_member)                \
    do {                                                                       \
        if (!(stream) || !(member))                                            \
            return ZarrError_InvalidArgument;                                  \
        size_t nbytes = strnlen(member, bytes_of_member);                      \
        stream->member = { member, nbytes };                                   \
        return ZarrError_Success;                                              \
    } while (0)

#define ZARR_STREAM_GET_STRING(stream, member)                                 \
    do {                                                                       \
        if (!stream)                                                           \
            return nullptr;                                                    \
        return stream->member.c_str();                                         \
    } while (0)

/* Create and destroy */
ZarrStreamSettings*
ZarrStreamSettings_create()
{
    return new ZarrStreamSettings();
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
    ZARR_STREAM_SET_STRING(settings, store_path, bytes_of_store_path);
}

ZarrError
ZarrStreamSettings_set_s3_endpoint(ZarrStreamSettings* settings,
                                   const char* s3_endpoint,
                                   size_t bytes_of_s3_endpoint)
{
    ZARR_STREAM_SET_STRING(settings, s3_endpoint, bytes_of_s3_endpoint);
}

ZarrError
ZarrStreamSettings_set_s3_bucket_name(ZarrStreamSettings* settings,
                                      const char* s3_bucket_name,
                                      size_t bytes_of_s3_bucket_name)
{
    ZARR_STREAM_SET_STRING(settings, s3_bucket_name, bytes_of_s3_bucket_name);
}

ZarrError
ZarrStreamSettings_set_s3_access_key_id(ZarrStreamSettings* settings,
                                        const char* s3_access_key_id,
                                        size_t bytes_of_s3_access_key_id)
{
    ZARR_STREAM_SET_STRING(
      settings, s3_access_key_id, bytes_of_s3_access_key_id);
}

ZarrError
ZarrStreamSettings_set_s3_secret_access_key(
  ZarrStreamSettings* settings,
  const char* s3_secret_access_key,
  size_t bytes_of_s3_secret_access_key)
{
    ZARR_STREAM_SET_STRING(
      settings, s3_secret_access_key, bytes_of_s3_secret_access_key);
}

ZarrError
ZarrStreamSettings_set_compressor(ZarrStreamSettings* settings,
                                  ZarrCompressor compressor)
{
    if (!settings || compressor >= ZarrCompressorCount)
        return ZarrError_InvalidArgument;

    if (compressor >= ZarrCompressor_Blosc2)
        return ZarrError_NotYetImplemented;

    settings->compressor = compressor;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_set_compression_codec(ZarrStreamSettings* settings,
                                         ZarrCompressionCodec codec)
{
    if (!settings || codec >= ZarrCompressionCodecCount)
        return ZarrError_InvalidArgument;

    settings->codec = codec;
    return ZarrError_Success;
}

ZarrError
ZarrStreamSettings_reserve_dimensions(ZarrStreamSettings* settings,
                                      size_t count)
{
    if (!settings || count < ZARR_DIMENSION_MIN || count > ZARR_DIMENSION_MAX)
        return ZarrError_InvalidArgument;

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
    if (!settings || !name || strnlen(name, bytes_of_name) == 0)
        return ZarrError_InvalidArgument;

    // Check that the index is within bounds
    if (index >= settings->dimensions.size())
        return ZarrError_InvalidIndex;

    // Check that the dimension type is valid
    if (kind >= ZarrDimensionTypeCount)
        return ZarrError_InvalidArgument;

    // Check that the chunk size is valid
    if (chunk_size_px == 0)
        return ZarrError_InvalidArgument;

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
    if (!settings)
        return ZarrError_InvalidArgument;

    settings->multiscale = multiscale > 0;
    return ZarrError_Success;
}

/* Getters */
const char*
ZarrStreamSettings_get_store_path(ZarrStreamSettings* settings)
{
    ZARR_STREAM_GET_STRING(settings, store_path);
}

const char*
ZarrStreamSettings_get_s3_endpoint(ZarrStreamSettings* settings)
{
    ZARR_STREAM_GET_STRING(settings, s3_endpoint);
}

const char*
ZarrStreamSettings_get_s3_bucket_name(ZarrStreamSettings* settings)
{
    ZARR_STREAM_GET_STRING(settings, s3_bucket_name);
}

const char*
ZarrStreamSettings_get_s3_access_key_id(ZarrStreamSettings* settings)
{
    ZARR_STREAM_GET_STRING(settings, s3_access_key_id);
}

const char*
ZarrStreamSettings_get_s3_secret_access_key(ZarrStreamSettings* settings)
{
    ZARR_STREAM_GET_STRING(settings, s3_secret_access_key);
}

ZarrCompressor
ZarrStreamSettings_get_compressor(ZarrStreamSettings* settings)
{
    if (!settings)
        return ZarrCompressor_None;
    return static_cast<ZarrCompressor>(settings->compressor);
}

ZarrCompressionCodec
ZarrStreamSettings_get_compression_codec(ZarrStreamSettings* settings)
{
    if (!settings)
        return ZarrCompressionCodec_None;
    return static_cast<ZarrCompressionCodec>(settings->codec);
}

size_t
ZarrStreamSettings_get_dimension_count(ZarrStreamSettings* settings)
{
    if (!settings)
        return 0;
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
    if (!settings || !name || !kind || !array_size_px || !chunk_size_px ||
        !shard_size_chunks)
        return ZarrError_InvalidArgument;

    if (index >= settings->dimensions.size())
        return ZarrError_InvalidIndex;

    struct ZarrDimension_s* dim = &settings->dimensions[index];

    if (bytes_of_name < dim->name.length() + 1)
        return ZarrError_Overflow;

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
    if (!settings)
        return 0;
    return static_cast<uint8_t>(settings->multiscale);
}

/* Internal functions */

bool
validate_dimension(const struct ZarrDimension_s& dimension)
{
    return !dimension.name.empty() && dimension.kind < ZarrDimensionTypeCount &&
           dimension.chunk_size_px > 0;
}
