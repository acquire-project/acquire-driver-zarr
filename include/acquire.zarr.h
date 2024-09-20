#pragma once

#include "zarr.types.h"

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct ZarrStreamSettings_s
    {
        ZarrS3Settings s3_settings;
        ZarrCompressionSettings compression_settings;
        ZarrDimensionProperties* dimensions;
        size_t dimension_count;
        char* store_path;
        char* custom_metadata;
        ZarrDataType data_type;
        ZarrVersion version;
        bool multiscale;
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
