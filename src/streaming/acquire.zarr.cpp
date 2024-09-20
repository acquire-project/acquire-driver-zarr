#include "zarr.types.h"
#include "zarr.stream.hh"
#include "macros.hh"

#include <cstdint> // uint32_t

#define ACQUIRE_ZARR_API_VERSION 0

extern "C"
{
    uint32_t Zarr_get_api_version()
    {
        return ACQUIRE_ZARR_API_VERSION;
    }

    ZarrStatusCode Zarr_set_log_level(ZarrLogLevel level)
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

    const char* Zarr_get_status_message(ZarrStatusCode error)
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

    ZarrStream_s* ZarrStream_create(struct ZarrStreamSettings_s* settings,
                                    ZarrVersion version)
    {

        ZarrStream_s* stream = nullptr;

        try {
            stream = new ZarrStream_s(settings, version);
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for Zarr stream");
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating Zarr stream: %s", e.what());
        }

        return stream;
    }

    void ZarrStream_destroy(struct ZarrStream_s* stream)
    {
        delete stream;
    }

    ZarrStatusCode ZarrStream_append(struct ZarrStream_s* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(data, "Null pointer: data");
        EXPECT_VALID_ARGUMENT(bytes_out, "Null pointer: bytes_out");

        try {
            *bytes_out = stream->append(data, bytes_in);
        } catch (const std::exception& e) {
            LOG_ERROR("Error appending data: %s", e.what());
            return ZarrStatus_InternalError;
        }

        return ZarrStatus_Success;
    }
}