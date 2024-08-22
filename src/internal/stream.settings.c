#include <stddef.h> // size_t
#include <stdlib.h> // malloc, free
#include <string.h> // memcpy

#include "stream.settings.h"
#include "zarr.h"

#define ZARR_STREAM_SET_STRING(stream, member, bytes_of_member)                \
    if (!stream || !member)                                                    \
        return ZarrError_InvalidArgument;                                      \
    size_t nbytes = strnlen(member, bytes_of_member) + 1;                      \
    if (nbytes >= sizeof(stream->member))                                      \
        return ZarrError_Overflow;                                             \
    memcpy(stream->member, member, nbytes);                                    \
    return ZarrError_Success;

#define ZARR_STREAM_GET_STRING(stream, member)                                 \
    if (!stream)                                                               \
        return 0;                                                              \
    return stream->member;

/* Create and destroy */
ZarrStreamSettings*
ZarrStreamSettings_create()
{
    ZarrStreamSettings* stream = malloc(sizeof(ZarrStreamSettings));
    if (!stream)
        return 0;
    memset(stream, 0, sizeof(*stream));
    return stream;
}

void
ZarrStreamSettings_destroy(ZarrStreamSettings* settings)
{
    free(settings);
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
ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
                                 size_t index,
                                 const char* name,
                                 size_t bytes_of_name,
                                 ZarrDimensionType kind,
                                 size_t array_size_px,
                                 size_t chunk_size_px,
                                 size_t shard_size_chunks)
{
    if (!settings || !name || name[0] == '\0')
        return ZarrError_InvalidArgument;

    // Check that the index is within bounds
    if (index >= sizeof(settings->dimensions.data) /
                   sizeof(settings->dimensions.data[0]))
        return ZarrError_InvalidIndex;

    // Check that the dimension type is valid
    if (kind >= ZarrDimensionTypeCount)
        return ZarrError_InvalidArgument;

    // Check that the chunk size is valid
    if (chunk_size_px == 0)
        return ZarrError_InvalidArgument;

    struct ZarrDimension_s* dim = &settings->dimensions.data[index];

    if (bytes_of_name >= sizeof(dim->name))
        return ZarrError_Overflow;

    memcpy(dim->name, name, bytes_of_name);
    dim->kind = kind;
    dim->array_size_px = array_size_px;
    dim->chunk_size_px = chunk_size_px;
    dim->shard_size_chunks = shard_size_chunks;
    ++settings->dimensions.count;
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
    return settings->compressor;
}

ZarrCompressionCodec
ZarrStreamSettings_get_compression_codec(ZarrStreamSettings* settings)
{
    if (!settings)
        return ZarrCompressionCodec_None;
    return settings->codec;
}

size_t
ZarrStreamSettings_get_dimension_count(ZarrStreamSettings* settings)
{
    if (!settings)
        return 0;
    return settings->dimensions.count;
}

static int
is_valid_dimension(ZarrStreamSettings* stream, size_t index)
{
    if (!stream || index >= sizeof(stream->dimensions.data) /
                              sizeof(stream->dimensions.data[0]))
        return 0;

    return stream->dimensions.data[index].name[0] != '\0';
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

    if (!is_valid_dimension(settings, index))
        return ZarrError_InvalidIndex;

    struct ZarrDimension_s* dim = &settings->dimensions.data[index];

    if (bytes_of_name < strnlen(dim->name, sizeof(dim->name)) + 1)
        return ZarrError_Overflow;

    memcpy(name, dim->name, strnlen(dim->name, sizeof(dim->name)) + 1);
    *kind = dim->kind;
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
    return settings->multiscale;
}

/* Internal functions */

uint8_t
validate_dimension(const struct ZarrDimension_s* dimension)
{
    if (!dimension)
        return 0;

    if (dimension->name[0] == '\0')
        return 0;

    if (dimension->kind >= ZarrDimensionTypeCount)
        return 0;

    if (dimension->chunk_size_px == 0)
        return 0;

    return 1;
}
