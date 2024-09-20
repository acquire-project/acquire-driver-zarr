#pragma once

#include "zarr.types.h"

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct ZarrStreamSettings_s
    {
        char* store_path; /**< Path to the store. Filesystem path or S3 key prefix. */
        char* custom_metadata; /**< JSON-formatted custom metadata to be stored with the dataset. */
        ZarrS3Settings* s3_settings; /**< Optional S3 settings for the store. */
        ZarrCompressionSettings* compression_settings; /**< Optional chunk compression settings for the store. */
        ZarrDimensionProperties* dimensions; /**< The properties of each dimension in the dataset. */
        size_t dimension_count; /**< The number of dimensions in the dataset. */
        bool multiscale; /**< Whether to stream to multiple levels of detail. */
        ZarrDataType data_type; /**< The pixel data type of the dataset. */
        ZarrVersion version; /**< The version of the Zarr format to use. 2 or 3. */
    } ZarrStreamSettings;

    typedef struct ZarrStream_s ZarrStream;

    /**
     * @brief Get the version of the Zarr API.
     * @return The version of the Zarr API.
     */
    uint32_t Zarr_get_api_version();

    /**
     * @brief Set the log level for the Zarr API.
     * @param level The log level.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatusCode Zarr_set_log_level(ZarrLogLevel level);

    /**
     * @brief Get the log level for the Zarr API.
     * @return The log level for the Zarr API.
     */
    ZarrLogLevel Zarr_get_log_level();

    /**
     * @brief Get the message for the given status code.
     * @param status The status code.
     * @return A human-readable status message.
     */
    const char* Zarr_get_status_message(ZarrStatusCode status);

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
     * @details This function will block while chunks are compressed and written
     * to the store. It will return when all data has been written.
     * @param[in, out] stream The Zarr stream struct.
     * @param[in] data The data to append.
     * @param[in] bytes_in The number of bytes in @p data. It should be at least
     * the size of a single frame.
     * @param[out] bytes_out The number of bytes written to the stream.
     * @return ZarrStatus_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStream_append(ZarrStream* stream,
                                 const void* data,
                                 size_t bytes_in,
                                 size_t* bytes_out);

#ifdef __cplusplus
}
#endif
