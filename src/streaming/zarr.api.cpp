#include "zarr.types.h"
#include "macros.hh"

#include <cstdint> // uint32_t

#define ACQUIRE_ZARR_API_VERSION 0

extern "C"
{
    uint32_t Zarr_get_api_version()
    {
        return ACQUIRE_ZARR_API_VERSION;
    }

    const char* Zarr_get_error_message(ZarrStatus error)
    {
        switch (error) {
            case ZarrStatus_Success:
                return "Success";
            case ZarrStatus_InvalidArgument:
                return "Invalid argument";
            case ZarrStatus_Overflow:
                return "Buffer overflow";
            case ZarrStatus_InvalidIndex:
                return "Invalid index";
            case ZarrStatus_NotYetImplemented:
                return "Not yet implemented";
            case ZarrStatus_InternalError:
                return "Internal error";
            case ZarrStatus_OutOfMemory:
                return "Out of memory";
            case ZarrStatus_IOError:
                return "I/O error";
            case ZarrStatus_CompressionError:
                return "Compression error";
            case ZarrStatus_InvalidSettings:
                return "Invalid settings";
            default:
                return "Unknown error";
        }
    }

    ZarrStatus Zarr_set_log_level(ZarrLogLevel level)
    {
        EXPECT_VALID_ARGUMENT(
          level < ZarrLogLevelCount, "Invalid log level: %d", level);

        Logger::set_log_level(level);
        return ZarrStatus_Success;
    }

    ZarrLogLevel Zarr_get_log_level()
    {
        return Logger::get_log_level();
    }
}