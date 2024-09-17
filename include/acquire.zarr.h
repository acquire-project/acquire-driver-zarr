#ifndef H_ACQUIRE_ZARR_V0
#define H_ACQUIRE_ZARR_V0

#include "zarr.types.h"

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct ZarrStreamSettings_s ZarrStreamSettings;
    typedef struct ZarrStream_s ZarrStream;

    /**
     * @brief Get the version of the Zarr API.
     * @return The version of the Zarr API.
     */
    uint32_t Zarr_get_api_version();

    /**
     * @brief Get the message for the given status code.
     * @param status The status code.
     * @return A human-readable status message.
     */
    const char* Zarr_get_error_message(ZarrStatus status);

    /**
     * @brief Create a Zarr stream settings struct.
     * @return A pointer to the Zarr stream settings struct, or NULL on failure.
     */
    ZarrStreamSettings* ZarrStreamSettings_create();

    /**
     * @brief Destroy a Zarr stream settings struct.
     * @details This function frees the memory allocated for the Zarr stream
     * settings struct.
     * @param[in] settings The Zarr stream settings struct.
     */
    void ZarrStreamSettings_destroy(ZarrStreamSettings* settings);

    /**
     * @brief Copy a Zarr stream settings struct.
     * @param[in] settings The Zarr stream settings struct to copy.
     * @return A copy of the Zarr stream settings struct.
     */
    ZarrStreamSettings* ZarrStreamSettings_copy(
      const ZarrStreamSettings* settings);

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
     * @brief Set the data type, compressor, codec, compression_settings level,
     * and shuffle for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] compression_settings The compression_settings settings.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_compression(
      ZarrStreamSettings* settings,
      const ZarrCompressionSettings* compression_settings);

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
     * @param[in] dimension The dimension's settings.
     */
    ZarrStatus ZarrStreamSettings_set_dimension(
      ZarrStreamSettings* settings,
      size_t index,
      const ZarrDimensionProperties* dimension);

    /**
     * @brief Set the multiscale flag for the Zarr stream.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param[in] multiscale A flag indicating whether to stream to multiple
     * levels of detail.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_multiscale(ZarrStreamSettings* settings,
                                                 uint8_t multiscale);

    /**
     * @brief Set JSON-formatted custom metadata for the Zarr stream.
     * @details This metadata will be written to acquire-zarr.json in the
     * metadata directory of the Zarr store. This parameter is optional.
     * @param settings[in, out] settings The Zarr stream settings struct.
     * @param external_metadata JSON-formatted external metadata.
     * @param bytes_of_external_metadata The length of @p custom_metadata in
     * bytes, including the null terminator.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatus ZarrStreamSettings_set_custom_metadata(
      ZarrStreamSettings* settings,
      const char* external_metadata,
      size_t bytes_of_external_metadata);

    const char* ZarrStreamSettings_get_store_path(
      const ZarrStreamSettings* settings);

    ZarrS3Settings ZarrStreamSettings_get_s3_settings(
      const ZarrStreamSettings* settings);

    ZarrCompressionSettings ZarrStreamSettings_get_compression(
      const ZarrStreamSettings* settings);

    ZarrDataType ZarrStreamSettings_get_data_type(
      const ZarrStreamSettings* settings);

    size_t ZarrStreamSettings_get_dimension_count(
      const ZarrStreamSettings* settings);

    ZarrDimensionProperties ZarrStreamSettings_get_dimension(
      const ZarrStreamSettings* settings,
      size_t index);

    bool ZarrStreamSettings_get_multiscale(const ZarrStreamSettings* settings);

    const char* ZarrStreamSettings_get_custom_metadata(
      const ZarrStreamSettings* settings);

    /**
     * @brief Create a Zarr stream.
     * @param[in, out] settings The settings for the Zarr stream.
     * @param[in] version The version of the Zarr stream. 2 or 3.
     * @return A pointer to the Zarr stream struct, or NULL on failure.
     */
    ZarrStream* ZarrStream_create(ZarrStreamSettings* settings,
                                  ZarrVersion version);

    /**
     * @brief Destroy a Zarr stream.
     * @details This function frees the memory allocated for the Zarr stream.
     * @param stream The Zarr stream struct to destroy.
     */
    void ZarrStream_destroy(ZarrStream* stream);

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

    /**
     * @brief Get the version (i.e., 2 or 3) of the Zarr stream.
     * @param stream The Zarr stream struct.
     * @return The version of the Zarr stream.
     */
    ZarrVersion ZarrStream_get_version(const ZarrStream* stream);

    /**
     * @brief Get a copy of the settings for the Zarr stream.
     * @param stream The Zarr stream struct.
     * @return A copy of the settings for the Zarr stream.
     */
    ZarrStreamSettings* ZarrStream_get_settings(const ZarrStream* stream);

    ZarrStatus Zarr_set_log_level(ZarrLogLevel level);
    ZarrLogLevel Zarr_get_log_level();

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_V0
