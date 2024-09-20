#include "acquire.zarr.h"
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
        return ZarrStatusCode_Success;
    }

    ZarrLogLevel Zarr_get_log_level()
    {
        return Logger::get_log_level();
    }

    const char* Zarr_get_status_message(ZarrStatusCode error)
    {
        switch (error) {
            case ZarrStatusCode_Success:
                return "Success";
            case ZarrStatusCode_InvalidArgument:
                return "Invalid argument";
            case ZarrStatusCode_Overflow:
                return "Buffer overflow";
            case ZarrStatusCode_InvalidIndex:
                return "Invalid index";
            case ZarrStatusCode_NotYetImplemented:
                return "Not yet implemented";
            case ZarrStatusCode_InternalError:
                return "Internal error";
            case ZarrStatusCode_OutOfMemory:
                return "Out of memory";
            case ZarrStatusCode_IOError:
                return "I/O error";
            case ZarrStatusCode_CompressionError:
                return "Compression error";
            case ZarrStatusCode_InvalidSettings:
                return "Invalid settings";
            default:
                return "Unknown error";
        }
    }

    ZarrStatusCode ZarrStreamSettings_create_dimension_array(
      struct ZarrStreamSettings_s* settings,
      size_t dimension_count)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(dimension_count >= 3,
                              "Invalid dimension count: %zu",
                              dimension_count);

        ZarrDimensionProperties* dimensions = nullptr;

        try {
            dimensions = new ZarrDimensionProperties[dimension_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for dimensions");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrStreamSettings_destroy_dimension_array(settings);
        settings->dimensions = dimensions;
        settings->dimension_count = dimension_count;

        return ZarrStatusCode_Success;
    }

    void ZarrStreamSettings_destroy_dimension_array(
      struct ZarrStreamSettings_s* settings)
    {
        if (nullptr != settings && nullptr != settings->dimensions) {
            delete[] settings->dimensions;
            settings->dimensions = nullptr;
        }
    }

    ZarrStream_s* ZarrStream_create(struct ZarrStreamSettings_s* settings)
    {

        ZarrStream_s* stream = nullptr;

        try {
            stream = new ZarrStream_s(settings);
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
            return ZarrStatusCode_InternalError;
        }

        return ZarrStatusCode_Success;
    }
}