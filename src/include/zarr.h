#ifndef H_ACQUIRE_ZARR_V0
#define H_ACQUIRE_ZARR_V0

#include <stddef.h> // size_t
#include <stdint.h> // uint8_t

#ifdef __cplusplus
extern "C"
{
#endif
    /***************************************************************************
     * Log level
     *
     * The LogLevel enum lists the available log levels. Use LogLevel_None to
     * suppress all log messages.
     **************************************************************************/
    enum LogLevel
    {
        LogLevel_Debug,
        LogLevel_Info,
        LogLevel_Warning,
        LogLevel_Error,
        LogLevel_None,
        LogLevelCount
    };

    /***************************************************************************
     * Zarr stream settings
     *
     * The Zarr stream settings struct is created using
     * ZarrStreamSettings_create and destroyed using ZarrStreamSettings_destroy.
     * If the struct fails to be created, ZarrStreamSettings_create returns
     * NULL.
     **************************************************************************/

    typedef struct ZarrStreamSettings_s ZarrStreamSettings;

    /***************************************************************************
     * Data type
     *
     * The ZarrDataType enum lists the available pixel types.
     **************************************************************************/

    typedef enum
    {
        ZarrDataType_uint8,
        ZarrDataType_uint16,
        ZarrDataType_uint32,
        ZarrDataType_uint64,
        ZarrDataType_int8,
        ZarrDataType_int16,
        ZarrDataType_int32,
        ZarrDataType_int64,
        ZarrDataType_float16,
        ZarrDataType_float32,
        ZarrDataType_float64,
        ZarrDataTypeCount
    } ZarrDataType;

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
     * Error handling
     *
     * The ZarrError enum lists the available error codes. The Zarr_get_error_message
     * function returns a human-readable error message for the given error code.
     **************************************************************************/
    typedef enum
    {
        ZarrError_Success = 0,
        ZarrError_InvalidArgument,
        ZarrError_Overflow,
        ZarrError_InvalidIndex,
        ZarrError_NotYetImplemented,
        ZarrError_Failure,
        ZarrError_InternalError,
        ZarrErrorCount,
    } ZarrError;

    const char* Zarr_get_error_message(ZarrError error);

    /***************************************************************************
     * Functions for creating and destroying a Zarr stream settings struct.
     *
     * ZarrStreamSettings is an opaque data structure that holds the parameters
     * for the Zarr stream. The struct is created using
     * ZarrStreamSettings_create and destroyed using ZarrStreamSettings_destroy.
     * If the struct fails to be created, ZarrStreamSettings_create returns
     * NULL.
     **************************************************************************/

    ZarrStreamSettings* ZarrStreamSettings_create();
    void ZarrStreamSettings_destroy(ZarrStreamSettings* settings);

    /***************************************************************************
     * Functions for setting parameters for a Zarr stream.
     *
     * These parameters are used to configure the Zarr stream.
     * Each function returns a ZarrError to indicate success or failure, which
     * can be converted to a human-readable error message using
     * Zarr_get_error_message(). If the function fails, the settings struct is
     * not modified.
     **************************************************************************/

    ZarrError ZarrStreamSettings_set_store_path(ZarrStreamSettings* settings,
                                                const char* store_path,
                                                size_t bytes_of_store_path);
    ZarrError ZarrStreamSettings_set_s3_endpoint(ZarrStreamSettings* settings,
                                                 const char* s3_endpoint,
                                                 size_t bytes_of_s3_endpoint);
    ZarrError ZarrStreamSettings_set_s3_bucket_name(
      ZarrStreamSettings* settings,
      const char* s3_bucket_name,
      size_t bytes_of_s3_bucket_name);
    ZarrError ZarrStreamSettings_set_s3_access_key_id(
      ZarrStreamSettings* settings,
      const char* s3_access_key_id,
      size_t bytes_of_s3_access_key_id);
    ZarrError ZarrStreamSettings_set_s3_secret_access_key(
      ZarrStreamSettings* settings,
      const char* s3_secret_access_key,
      size_t bytes_of_s3_secret_access_key);

    ZarrError ZarrStreamSettings_set_data_type(ZarrStreamSettings* settings,
                                               ZarrDataType pixel_type);

    ZarrError ZarrStreamSettings_set_compressor(ZarrStreamSettings* settings,
                                                ZarrCompressor compressor);
    ZarrError ZarrStreamSettings_set_compression_codec(
      ZarrStreamSettings* settings,
      ZarrCompressionCodec codec);
    ZarrError ZarrStreamSettings_set_compression_level(
      ZarrStreamSettings* settings,
      uint8_t compression_level);
    ZarrError ZarrStreamSettings_set_compression_shuffle(
      ZarrStreamSettings* settings,
      uint8_t shuffle);

    ZarrError ZarrStreamSettings_reserve_dimensions(
      ZarrStreamSettings* settings,
      size_t count);
    ZarrError ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
                                               size_t index,
                                               const char* name,
                                               size_t bytes_of_name,
                                               ZarrDimensionType kind,
                                               uint32_t array_size_px,
                                               uint32_t chunk_size_px,
                                               uint32_t shard_size_chunks);

    ZarrError ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
                                                uint8_t multiscale);

    /***************************************************************************
     * Functions for getting parameters on the Zarr stream settings struct.
     *
     * These functions return the value of the specified parameter.
     * If the struct is NULL, the functions return NULL or 0.
     **************************************************************************/

    const char* ZarrStreamSettings_get_store_path(ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_endpoint(
      ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_bucket_name(
      ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_access_key_id(
      ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_secret_access_key(
      ZarrStreamSettings* settings);

    ZarrDataType ZarrStreamSettings_get_data_type(ZarrStreamSettings* settings);

    ZarrCompressor ZarrStreamSettings_get_compressor(
      ZarrStreamSettings* settings);
    ZarrCompressionCodec ZarrStreamSettings_get_compression_codec(
      ZarrStreamSettings* settings);
    uint8_t ZarrStreamSettings_get_compression_level(
      ZarrStreamSettings* settings);
    uint8_t ZarrStreamSettings_get_compression_shuffle(
      ZarrStreamSettings* settings);

    size_t ZarrStreamSettings_get_dimension_count(ZarrStreamSettings* settings);
    ZarrError ZarrStreamSettings_get_dimension(ZarrStreamSettings* settings,
                                               size_t index,
                                               char* name,
                                               size_t bytes_of_name,
                                               ZarrDimensionType* kind,
                                               size_t* array_size_px,
                                               size_t* chunk_size_px,
                                               size_t* shard_size_chunks);

    uint8_t ZarrStreamSettings_get_multiscale(ZarrStreamSettings* settings);

    /***************************************************************************
     * Zarr stream
     *
     * The Zarr stream struct is created using ZarrStream_create and destroyed
     * using ZarrStream_destroy. If the struct fails to be created,
     * ZarrStream_create returns NULL.
     **************************************************************************/

    typedef enum
    {
        ZarrVersion_2 = 2,
        ZarrVersion_3,
        ZarrVersionCount
    } ZarrVersion;

    typedef struct ZarrStream_s ZarrStream;

    ZarrStream* ZarrStream_create(ZarrStreamSettings* settings,
                                  ZarrVersion version);
    void ZarrStream_destroy(ZarrStream* stream);

    /***************************************************************************
     * Writing data to the Zarr stream.
     *
     * This function writes data to the Zarr stream. It returns a ZarrError to
     * indicate success or failure, which can be converted to a human-readable
     * error message using Zarr_get_error_message().
     **************************************************************************/

    ZarrError ZarrStream_append(ZarrStream* stream,
                                const void* data,
                                size_t bytes_in,
                                size_t* bytes_out);

    /***************************************************************************
     * Functions for getting parameters on the Zarr stream.
     *
     * These functions return the value of the specified parameter.
     * If the Zarr stream is NULL, the functions return NULL or 0.
     **************************************************************************/

    ZarrVersion ZarrStream_get_version(ZarrStream* stream);

    const char* ZarrStream_get_store_path(ZarrStream* stream);
    const char* ZarrStream_get_s3_endpoint(ZarrStream* stream);
    const char* ZarrStream_get_s3_bucket_name(ZarrStream* stream);
    const char* ZarrStream_get_s3_access_key_id(ZarrStream* stream);
    const char* ZarrStream_get_s3_secret_access_key(ZarrStream* stream);

    ZarrCompressor ZarrStream_get_compressor(ZarrStream* stream);
    ZarrCompressionCodec ZarrStream_get_compression_codec(ZarrStream* stream);
    uint8_t ZarrStream_get_compression_level(ZarrStream* stream);
    uint8_t ZarrStream_get_compression_shuffle(ZarrStream* stream);

    size_t ZarrStream_get_dimension_count(ZarrStream* stream);
    ZarrError ZarrStream_get_dimension(ZarrStream* stream,
                                       size_t index,
                                       char* name,
                                       size_t bytes_of_name,
                                       ZarrDimensionType* kind,
                                       size_t* array_size_px,
                                       size_t* chunk_size_px,
                                       size_t* shard_size_chunks);

    uint8_t ZarrStream_get_multiscale(ZarrStream* stream);

    /***************************************************************************
     * Logging
     *
     * The Zarr library uses a logging system to output messages. The log level
     * can be set using Zarr_set_log_level. The log level can be retrieved using
     * Zarr_get_log_level. The log level can be set to one of the values in the
     * in the LogLevel enum. Use LogLevel_None to suppress all log messages. By
     * default, the log level is set to LogLevel_Info.
     **************************************************************************/

    ZarrError Zarr_set_log_level(LogLevel level);
    LogLevel Zarr_get_log_level();

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_V0
