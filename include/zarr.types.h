#ifndef H_ACQUIRE_ZARR_TYPES_V0
#define H_ACQUIRE_ZARR_TYPES_V0

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef enum
    {
        ZarrStatus_Success = 0,
        ZarrStatus_InvalidArgument,
        ZarrStatus_Overflow,
        ZarrStatus_InvalidIndex,
        ZarrStatus_NotYetImplemented,
        ZarrStatus_InternalError,
        ZarrStatus_OutOfMemory,
        ZarrStatus_IOError,
        ZarrStatus_CompressionError,
        ZarrStatus_InvalidSettings,
        ZarrStatusCount,
    } ZarrStatus;

    typedef enum
    {
        ZarrVersion_2 = 2,
        ZarrVersion_3,
        ZarrVersionCount
    } ZarrVersion;

    typedef enum
    {
        ZarrLogLevel_Debug,
        ZarrLogLevel_Info,
        ZarrLogLevel_Warning,
        ZarrLogLevel_Error,
        ZarrLogLevel_None,
        ZarrLogLevelCount
    } ZarrLogLevel;

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
        ZarrDataType_float32,
        ZarrDataType_float64,
        ZarrDataTypeCount
    } ZarrDataType;

    typedef enum
    {
        ZarrCompressor_None = 0,
        ZarrCompressor_Blosc1,
        ZarrCompressorCount
    } ZarrCompressor;

    typedef enum
    {
        ZarrCompressionCodec_None = 0,
        ZarrCompressionCodec_BloscLZ4,
        ZarrCompressionCodec_BloscZstd,
        ZarrCompressionCodecCount
    } ZarrCompressionCodec;

    typedef enum
    {
        ZarrDimensionType_Space = 0,
        ZarrDimensionType_Channel,
        ZarrDimensionType_Time,
        ZarrDimensionType_Other,
        ZarrDimensionTypeCount
    } ZarrDimensionType;

    /**
     * @brief S3 settings for streaming to Zarr.
     */
    typedef struct
    {
        const char* endpoint;
        size_t bytes_of_endpoint;
        const char* bucket_name;
        size_t bytes_of_bucket_name;
        const char* access_key_id;
        size_t bytes_of_access_key_id;
        const char* secret_access_key;
        size_t bytes_of_secret_access_key;
    } ZarrS3Settings;

    /**
     * @brief Compression settings for a Zarr array.
     * @detail The compressor is not the same as the codec. A codec is
     * a specific implementation of a compression algorithm, while a compressor
     * is a library that implements one or more codecs.
     */
    typedef struct
    {
        ZarrCompressor compressor;  /**< Compressor to use */
        ZarrCompressionCodec codec; /**< Codec to use */
        uint8_t level;              /**< Compression level */
        uint8_t shuffle; /**< Whether to shuffle the data before compressing */
    } ZarrCompressionSettings;

    /**
     * @brief Properties of a dimension of the Zarr array.
     */
    typedef struct
    {
        const char* name; /**< Name of the dimension */
        size_t bytes_of_name; /**< Bytes in @p name, including null terminator */
        ZarrDimensionType kind; /**< Type of the dimension */
        uint32_t array_size_px; /**< Size of the array along this dimension in
                                   pixels */
        uint32_t chunk_size_px; /**< Size of the chunks along this dimension in
                                   pixels */
        uint32_t shard_size_chunks; /**< Number of chunks in a shard along this
                                       dimension */
    } ZarrDimensionProperties;

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_TYPES_V0