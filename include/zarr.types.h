#ifndef H_ACQUIRE_ZARR_TYPES_V0
#define H_ACQUIRE_ZARR_TYPES_V0

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
        LogLevel_Debug,
        LogLevel_Info,
        LogLevel_Warning,
        LogLevel_Error,
        LogLevel_None,
        LogLevelCount
    } LogLevel;

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

    typedef struct
    {
        ZarrCompressor compressor;
        ZarrCompressionCodec codec;
        uint8_t level;
        uint8_t shuffle;
    } ZarrCompressionSettings;

    typedef struct
    {
        const char* name;
        size_t bytes_of_name;
        ZarrDimensionType kind;
        uint32_t array_size_px;
        uint32_t chunk_size_px;
        uint32_t shard_size_chunks;
    } ZarrDimensionSettings;

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_TYPES_V0