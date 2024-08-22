#include "zarr_errors.h"

const char*
Zarr_get_error_message(ZarrError error)
{
    switch (error) {
        case ZarrError_Success:
            return "Success";
        case ZarrError_InvalidArgument:
            return "Invalid argument";
        case ZarrError_Overflow:
            return "Overflow";
        case ZarrError_InvalidIndex:
            return "Invalid index";
        case ZarrError_NotYetImplemented:
            return "Not yet implemented";
        default:
            return "Unknown error";
    }
}
