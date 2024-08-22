#ifndef H_ACQUIRE_ZARR_V0
#define H_ACQUIRE_ZARR_V0

#include <stddef.h> // size_t

#include "zarr_errors.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /***************************************************************************
     * Zarr stream settings
     *
     * The Zarr stream settings struct is created using
     * ZarrStreamSettings_create and destroyed using ZarrStreamSettings_destroy.
     * If the struct fails to be created, ZarrStreamSettings_create returns
     *NULL.
     **************************************************************************/
    typedef struct ZarrStreamSettings_s ZarrStreamSettings;

    /***************************************************************************
     * Compression
     *
     * Acquire Zarr uses libraries to compress data streams. The ZarrCompressor
     * enum lists the available compressors. Note that a compressor is not
     * the same as a codec. A codec is a specific implementation of a
     * compression algorithm, while a compressor is a library that implements
     * one or more codecs. The ZarrCompressor enum lists the available
     * compressors. The ZarrCompressor_None value indicates that no compression
     * is used.
     *
     * The ZarrCompressionCodec enum lists the available codecs. The
     * ZarrCompressionCodec_None value should only be used when not compressing.
     * If the compressor is set to ZarrCompressor_None, the codec is ignored.
     **************************************************************************/

    typedef enum
    {
        ZarrCompressor_None = 0,
        ZarrCompressor_Blosc1,
        ZarrCompressor_Blosc2, /* Blosc2 is not yet supported */
        ZarrCompressor_Zstd,   /* Zstd is not yet supported */
        ZarrCompressorCount
    } ZarrCompressor;

    typedef enum
    {
        ZarrCompressionCodec_None = 0,
        ZarrCompressionCodec_BloscLZ4,
        ZarrCompressionCodec_BloscZstd,
        ZarrCompressionCodecCount
    } ZarrCompressionCodec;

    /***************************************************************************
     * Dimension types
     *
     * The ZarrDimensionType enum lists the available dimension types. The
     * ZarrDimensionType_Space value indicates that the dimension is a spatial
     * dimension. The ZarrDimensionType_Time value indicates that the dimension
     * is a time dimension. The ZarrDimensionType_Channel value indicates that
     * the dimension is a channel dimension. ZarrDimensionType_Other value
     * indicates that the dimension is not a spatial, time, or channel
     * dimension.
     **************************************************************************/

    typedef enum
    {
        ZarrDimensionType_Space = 0,
        ZarrDimensionType_Time,
        ZarrDimensionType_Channel,
        ZarrDimensionType_Other,
        ZarrDimensionTypeCount
    } ZarrDimensionType;

    /***************************************************************************
     * Functions for creating and destroying a Zarr stream settings struct.
     *
     * ZarrStreamSettings is an opaque data structure that holds the parameters
     * for the Zarr stream. The struct is created using
     * ZarrStreamSettings_create and destroyed using ZarrStreamSettings_destroy.
     * If the struct fails to be created, ZarrStreamSettings_create returns
     *NULL.
     **************************************************************************/

    ZarrStreamSettings* ZarrStreamSettings_create();
    void ZarrStreamSettings_destroy(ZarrStreamSettings* stream);

    /***************************************************************************
     * Functions for setting parameters for a Zarr stream.
     *
     * These parameters are used to configure the Zarr stream.
     * Each function returns a ZarrError to indicate success or failure, which
     * can be converted to a human-readable error message using
     * Zarr_get_error_message(). If the function fails, the settings struct is
     * not modified.
     **************************************************************************/

    ZarrError ZarrStreamSettings_set_store_path(ZarrStreamSettings* stream,
                                                const char* store_path,
                                                size_t bytes_of_store_path);
    ZarrError ZarrStreamSettings_set_s3_endpoint(ZarrStreamSettings* stream,
                                                 const char* s3_endpoint,
                                                 size_t bytes_of_s3_endpoint);
    ZarrError ZarrStreamSettings_set_s3_bucket_name(
      ZarrStreamSettings* stream,
      const char* s3_bucket_name,
      size_t bytes_of_s3_bucket_name);
    ZarrError ZarrStreamSettings_set_s3_access_key_id(
      ZarrStreamSettings* stream,
      const char* s3_access_key_id,
      size_t bytes_of_s3_access_key_id);
    ZarrError ZarrStreamSettings_set_s3_secret_access_key(
      ZarrStreamSettings* stream,
      const char* s3_secret_access_key,
      size_t bytes_of_s3_secret_access_key);

    ZarrError ZarrStreamSettings_set_compressor(ZarrStreamSettings* stream,
                                                ZarrCompressor compressor);
    ZarrError ZarrStreamSettings_set_compression_codec(
      ZarrStreamSettings* stream,
      ZarrCompressionCodec codec);

    ZarrError ZarrStreamSettings_set_dimension(ZarrStreamSettings* stream,
                                               size_t index,
                                               const char* name,
                                               ZarrDimensionType kind,
                                               size_t array_size_px,
                                               size_t chunk_size_px,
                                               size_t shard_size_chunks);

    /***************************************************************************
     * Functions for getting parameters on the Zarr stream.
     *
     * These functions return the value of the specified parameter.
     * If the Zarr stream is NULL, the functions return NULL or 0.
     **************************************************************************/

    const char* ZarrStreamSettings_get_store_path(ZarrStreamSettings* stream);
    const char* ZarrStreamSettings_get_s3_endpoint(ZarrStreamSettings* stream);
    const char* ZarrStreamSettings_get_s3_bucket_name(
      ZarrStreamSettings* stream);
    const char* ZarrStreamSettings_get_s3_access_key_id(
      ZarrStreamSettings* stream);
    const char* ZarrStreamSettings_get_s3_secret_access_key(
      ZarrStreamSettings* stream);

    ZarrCompressor ZarrStreamSettings_get_compressor(
      ZarrStreamSettings* stream);
    ZarrCompressionCodec ZarrStreamSettings_get_compression_codec(
      ZarrStreamSettings* stream);

    size_t ZarrStreamSettings_get_dimension_count(ZarrStreamSettings* stream);
    ZarrError ZarrStreamSettings_get_dimension(ZarrStreamSettings* stream,
                                               size_t index,
                                               char* name,
                                               size_t bytes_of_name,
                                               ZarrDimensionType* kind,
                                               size_t* array_size_px,
                                               size_t* chunk_size_px,
                                               size_t* shard_size_chunks);

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_V0
