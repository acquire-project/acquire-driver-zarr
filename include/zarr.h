#ifndef H_ACQUIRE_ZARR_V0
#define H_ACQUIRE_ZARR_V0

#include "zarr.types.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * @brief Get the version of the Zarr API.
     * @return The version of the Zarr API.
     */
    uint32_t Zarr_get_api_version();

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

    /***************************************************************************
     * Error handling
     *
     * The ZarrStatus enum lists the available status codes. The
     * Zarr_get_error_message function returns a human-readable status message
     * for the given status code.
     **************************************************************************/

    /**
     * @brief Get the message for the given status code.
     * @param status The status code.
     * @return A human-readable status message.
     */
    const char* Zarr_get_error_message(ZarrStatus status);

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
     * Each function returns a ZarrStatus to indicate success or failure, which
     * can be converted to a human-readable error message using
     * Zarr_get_error_message(). If the function fails, the settings struct is
     * not modified.
     **************************************************************************/

    /**
     * @brief Set store path and S3 settings for the Zarr stream.
     * @param[in, out] settings
     * @param[in] store_path The store path for the Zarr stream. Directory path
     * when acquiring to the filesystem, key prefix when acquiring to S3.
     * @param[in] bytes_of_store_path The length of @p store_path in bytes,
     * including the null terminator.
     * @param[in] s3_settings Optional S3 settings. If NULL, the store path is
     * assumed to be a directory path.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_store(ZarrStreamSettings* settings,
                                            const char* store_path,
                                            size_t bytes_of_store_path,
                                            const ZarrS3Settings* s3_settings);

    /**
     * @brief Set the data type, compressor, codec, compression_settings level, and
     * shuffle for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] compression_settings The compression_settings settings.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_compression(
      ZarrStreamSettings* settings,
      const ZarrCompressionSettings* compression_settings);

    /**
     * @brief Set JSON-formatted external metadata for the Zarr stream.
     * @details This metadata will be written to acquire-zarr.json in the
     * metadata directory of the Zarr store. This parameter is optional.
     * @param settings[in, out] settings The Zarr stream settings struct.
     * @param external_metadata JSON-formatted external metadata.
     * @param bytes_of_external_metadata The length of @p external_metadata in
     * bytes, including the null terminator.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_external_metadata(
      ZarrStreamSettings* settings,
      const char* external_metadata,
      size_t bytes_of_external_metadata);

    /**
     * @brief Set the data type for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] data_type The data type.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_data_type(ZarrStreamSettings* settings,
                                                ZarrDataType data_type);

    /**
     * @brief Reserve space for dimensions in the Zarr stream settings struct.
     * @detail *Must* precede calls to ZarrStreamSettings_set_dimension. We
     * require at least 3 dimensions to validate settings, but you may set up to
     * 32 dimensions.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] count The number of dimensions to reserve space for.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_reserve_dimensions(
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
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_dimension(ZarrStreamSettings* settings,
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
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
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
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_get_dimension(
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
     * This function writes data to the Zarr stream. It returns a ZarrStatus to
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
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStream_append(ZarrStream* stream,
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
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStream_get_dimension(const ZarrStream* stream,
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

    ZarrStatus Zarr_set_log_level(LogLevel level);
    LogLevel Zarr_get_log_level();

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_V0
