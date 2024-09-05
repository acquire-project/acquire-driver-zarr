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
    typedef enum
    {
        LogLevel_Debug,
        LogLevel_Info,
        LogLevel_Warning,
        LogLevel_Error,
        LogLevel_None,
        LogLevelCount
    } LogLevel;

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
        ZarrDimensionType_Channel,
        ZarrDimensionType_Time,
        ZarrDimensionType_Other,
        ZarrDimensionTypeCount
    } ZarrDimensionType;

    /***************************************************************************
     * Error handling
     *
     * The ZarrError enum lists the available error codes. The
     * Zarr_get_error_message function returns a human-readable error message
     * for the given error code.
     **************************************************************************/

    typedef enum
    {
        ZarrError_Success = 0,
        ZarrError_InvalidArgument,
        ZarrError_Overflow,
        ZarrError_InvalidIndex,
        ZarrError_NotYetImplemented,
        ZarrError_InternalError,
        ZarrErrorCount,
    } ZarrError;

    /**
     * @brief Get the error message for the given error code.
     * @param error The error code.
     * @return A human-readable error message.
     */
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

    /** @brief Return a pointer to a Zarr stream settings struct. */
    ZarrStreamSettings* ZarrStreamSettings_create();

    /** @brief Destroy a Zarr stream settings struct. Consumes the pointer */
    void ZarrStreamSettings_destroy(ZarrStreamSettings* settings);

    /** @brief Make a copy of the Zarr stream settings struct. */
    ZarrStreamSettings* ZarrStreamSettings_copy(
      const ZarrStreamSettings* settings);

    /***************************************************************************
     * Functions for setting parameters for a Zarr stream.
     *
     * These parameters are used to configure the Zarr stream.
     * Each function returns a ZarrError to indicate success or failure, which
     * can be converted to a human-readable error message using
     * Zarr_get_error_message(). If the function fails, the settings struct is
     * not modified.
     **************************************************************************/

    /**
     * @brief Set the store path for the Zarr stream.
     * @detail This parameter is required for all Zarr streams. When acquiring
     * to the filesystem, the store path is a directory path. When acquiring to
     * S3, the store path is a key prefix.
     * @param[in, out] setting The Zarr stream settings struct.
     * @param[in] store_path Path to the directory where the Zarr data is
     * stored, e.g., "/path/to/dataset.zarr".
     * @param[in] bytes_of_store_path The length of @p store_path in bytes,
     * including the null terminator.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_store_path(ZarrStreamSettings* settings,
                                                const char* store_path,
                                                size_t bytes_of_store_path);

    /**
     * @brief Set the S3 endpoint for the Zarr stream, if streaming to S3.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] s3_endpoint The S3 endpoint, e.g., "https://s3.amazonaws.com"
     * or "http://localhost:9000". Must begin with "http://" or "https://".
     * @param[in] bytes_of_s3_endpoint The length of @p s3_endpoint in bytes,
     * including the null terminator.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_s3_endpoint(ZarrStreamSettings* settings,
                                                 const char* s3_endpoint,
                                                 size_t bytes_of_s3_endpoint);

    /**
     * @brief Set the S3 bucket name for the Zarr stream, if streaming to S3.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] s3_bucket_name The S3 bucket name, e.g., "my-bucket".
     * @param[in] bytes_of_s3_bucket_name The length of @p s3_bucket_name in
     * bytes, including the null terminator.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_s3_bucket_name(
      ZarrStreamSettings* settings,
      const char* s3_bucket_name,
      size_t bytes_of_s3_bucket_name);

    /**
     * @brief Set the S3 access key ID for the Zarr stream, if streaming to S3.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] s3_access_key_id The access key ID.
     * @param[in] bytes_of_s3_access_key_id The length of @p s3_access_key_id in
     * bytes, including the null terminator.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_s3_access_key_id(
      ZarrStreamSettings* settings,
      const char* s3_access_key_id,
      size_t bytes_of_s3_access_key_id);

    /**
     * @brief Set the S3 secret access key for the Zarr stream, if streaming to
     * S3.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] s3_secret_access_key The secret access key.
     * @param[in] bytes_of_s3_secret_access_key The length of
     * @p s3_secret_access_key in bytes, including the null terminator.
     * @return
     */
    ZarrError ZarrStreamSettings_set_s3_secret_access_key(
      ZarrStreamSettings* settings,
      const char* s3_secret_access_key,
      size_t bytes_of_s3_secret_access_key);

    /**
     * @brief Set JSON-formatted external metadata for the Zarr stream.
     * @details This metadata will be written to acquire-zarr.json in the
     * metadata directory of the Zarr store. This parameter is optional.
     * @param settings[in, out] settings The Zarr stream settings struct.
     * @param external_metadata JSON-formatted external metadata.
     * @param bytes_of_external_metadata The length of @p external_metadata in
     * bytes, including the null terminator.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_external_metadata(
      ZarrStreamSettings* settings,
      const char* external_metadata,
      size_t bytes_of_external_metadata);

    /**
     * @brief Set the data type for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] data_type The data type.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_data_type(ZarrStreamSettings* settings,
                                               ZarrDataType data_type);

    /**
     * @brief Set the compressor for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] compressor Enum value for the compressor.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_compressor(ZarrStreamSettings* settings,
                                                ZarrCompressor compressor);

    /**
     * @brief Set the compression codec for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] codec Enum value for the compression codec.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_compression_codec(
      ZarrStreamSettings* settings,
      ZarrCompressionCodec codec);

    /**
     * @brief Set the compression level for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] compression_level A value between 0 and 9. Higher values
     * indicate higher compression levels. Set to 0 for no compression.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_compression_level(
      ZarrStreamSettings* settings,
      uint8_t compression_level);

    /**
     * @brief Set the compression shuffle value for the Zarr stream
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] shuffle A value of 0, 1, or (if using Blosc) 2. 0 indicates
     * no shuffle, 1 indicates shuffle, and 2 indicates bitshuffle.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_compression_shuffle(
      ZarrStreamSettings* settings,
      uint8_t shuffle);

    /**
     * @brief Reserve space for dimensions in the Zarr stream settings struct.
     * @detail *Must* precede calls to ZarrStreamSettings_set_dimension. We
     * require at least 3 dimensions to validate settings, but you may set up to
     * 32 dimensions.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] count The number of dimensions to reserve space for.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_reserve_dimensions(
      ZarrStreamSettings* settings,
      size_t count);

    /**
     * @brief Set properties for an acquisition dimension.
     * @detail The order of the dimensions in the Zarr stream is the order in
     * which they are set. The first dimension set is the slowest varying
     * dimension, and the last dimension set is the fastest varying dimension.
     * For example, if the dimensions are set in the order z, y, x, the fastest
     * varying dimension is x, the next fastest varying dimension is y, and the
     * slowest varying dimension is z.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] index The index of the dimension to set. Must be less than the
     * number of dimensions reserved with ZarrStreamSettings_reserve_dimensions.
     * @param[in] name The name of the dimension.
     * @param[in] bytes_of_name The length of @p name in bytes, including the
     * null terminator.
     * @param[in] kind The dimension type.
     * @param[in] array_size_px The size of the entire array along this
     * dimension, in pixels. This value must be positive for all dimensions
     * except the first (i.e., the slowest varying dimension).
     * @param[in] chunk_size_px The size of the chunks along this dimension, in
     * pixels. This value must be positive for all dimensions.
     * @param[in] shard_size_chunks The number of chunks in a shard. This value
     * must be positive for all dimensions but is ignored for Zarr V2 streams.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
                                               size_t index,
                                               const char* name,
                                               size_t bytes_of_name,
                                               ZarrDimensionType kind,
                                               uint32_t array_size_px,
                                               uint32_t chunk_size_px,
                                               uint32_t shard_size_chunks);

    /**
     * @brief Set the multiscale flag for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] multiscale A flag indicating whether to stream to multiple
     * levels of detail.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
                                                uint8_t multiscale);

    /***************************************************************************
     * Functions for getting parameters on the Zarr stream settings struct.
     *
     * These functions return the value of the specified parameter.
     * If the struct is NULL, the functions return NULL or 0.
     **************************************************************************/

    const char* ZarrStreamSettings_get_store_path(
      const ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_endpoint(
      const ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_bucket_name(
      const ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_access_key_id(
      const ZarrStreamSettings* settings);
    const char* ZarrStreamSettings_get_s3_secret_access_key(
      const ZarrStreamSettings* settings);

    const char* ZarrStreamSettings_get_external_metadata(
      const ZarrStreamSettings* settings);

    ZarrDataType ZarrStreamSettings_get_data_type(
      const ZarrStreamSettings* settings);

    ZarrCompressor ZarrStreamSettings_get_compressor(
      const ZarrStreamSettings* settings);
    ZarrCompressionCodec ZarrStreamSettings_get_compression_codec(
      const ZarrStreamSettings* settings);
    uint8_t ZarrStreamSettings_get_compression_level(
      const ZarrStreamSettings* settings);
    uint8_t ZarrStreamSettings_get_compression_shuffle(
      const ZarrStreamSettings* settings);

    size_t ZarrStreamSettings_get_dimension_count(
      const ZarrStreamSettings* settings);

    /**
     * @brief Get the properties for an acquisition dimension.
     * @param[in] settings The Zarr stream settings struct.
     * @param[in] index The index of the dimension to get. Must be less than the
     * number of dimensions reserved with ZarrStreamSettings_reserve_dimensions.
     * @param[out] name The name of the dimension.
     * @param[out] bytes_of_name The number of bytes in @p name. The function
     * will fail if this value is less than the number of bytes in the stored
     * name.
     * @param[out] kind The dimension type.
     * @param[out] array_size_px The size of the entire array along this
     * dimension, in pixels.
     * @param[out] chunk_size_px The size of a chunk along this dimension, in
     * pixels.
     * @param[out] shard_size_chunks The number of chunks in a shard.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStreamSettings_get_dimension(
      const ZarrStreamSettings* settings,
      size_t index,
      char* name,
      size_t bytes_of_name,
      ZarrDimensionType* kind,
      size_t* array_size_px,
      size_t* chunk_size_px,
      size_t* shard_size_chunks);

    uint8_t ZarrStreamSettings_get_multiscale(
      const ZarrStreamSettings* settings);

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

    /**
     * @brief Create a Zarr stream.
     * @param[in, out] settings The settings for the Zarr stream. This pointer
     * is consumed by this function.
     * @param[in] version The version of the Zarr stream. 2 or 3.
     * @return A pointer to the Zarr stream struct, or NULL on failure.
     */
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

    /**
     * @brief Append data to the Zarr stream.
     * @param[in, out] stream The Zarr stream struct.
     * @param[in] data The data to append.
     * @param[in] bytes_in The number of bytes in @p data. It should be at least
     * the size of a single frame.
     * @param[out] bytes_out The number of bytes written to the stream.
     * @return ZarrError_Success on success, or an error code on failure.
     */
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

    ZarrVersion ZarrStream_get_version(const ZarrStream* stream);

    const char* ZarrStream_get_store_path(const ZarrStream* stream);
    const char* ZarrStream_get_s3_endpoint(const ZarrStream* stream);
    const char* ZarrStream_get_s3_bucket_name(const ZarrStream* stream);
    const char* ZarrStream_get_s3_access_key_id(const ZarrStream* stream);
    const char* ZarrStream_get_s3_secret_access_key(const ZarrStream* stream);

    const char* ZarrStream_get_external_metadata(const ZarrStream* stream);

    ZarrCompressor ZarrStream_get_compressor(const ZarrStream* stream);
    ZarrCompressionCodec ZarrStream_get_compression_codec(
      const ZarrStream* stream);
    uint8_t ZarrStream_get_compression_level(const ZarrStream* stream);
    uint8_t ZarrStream_get_compression_shuffle(const ZarrStream* stream);

    size_t ZarrStream_get_dimension_count(const ZarrStream* stream);

    /**
     * @brief Get the properties for an acquisition dimension.
     * @param[in] stream The Zarr stream struct.
     * @param[in] index The index of the dimension to get. Must be less than the
     * number of dimensions reserved with ZarrStreamSettings_reserve_dimensions.
     * @param[out] name The name of the dimension.
     * @param[out] bytes_of_name The number of bytes in @p name. The function
     * will fail if this value is less than the number of bytes in the stored
     * name.
     * @param[out] kind The dimension type.
     * @param[out] array_size_px The size of the entire array along this
     * dimension, in pixels.
     * @param[out] chunk_size_px The size of a chunk along this dimension, in
     * pixels.
     * @param[out] shard_size_chunks The number of chunks in a shard.
     * @return ZarrError_Success on success, or an error code on failure.
     */
    ZarrError ZarrStream_get_dimension(const ZarrStream* stream,
                                       size_t index,
                                       char* name,
                                       size_t bytes_of_name,
                                       ZarrDimensionType* kind,
                                       size_t* array_size_px,
                                       size_t* chunk_size_px,
                                       size_t* shard_size_chunks);

    uint8_t ZarrStream_get_multiscale(const ZarrStream* stream);

    /** @brief Get a copy of the stream settings. */
    ZarrStreamSettings* ZarrStream_get_settings(const ZarrStream* stream);

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
