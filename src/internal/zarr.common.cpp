#include "zarr.common.hh"
#include "zarr_errors.h"
#include "logger.hh"

#include <stdexcept>

size_t
zarr::chunks_along_dimension(const Dimension& dimension)
{
    EXPECT(dimension.chunk_size_px > 0, "Invalid chunk size.");

    return (dimension.array_size_px + dimension.chunk_size_px - 1) /
           dimension.chunk_size_px;
}

size_t
zarr::shards_along_dimension(const Dimension& dimension)
{
    const size_t shard_size = dimension.shard_size_chunks;
    if (shard_size == 0)
        return 0;

    const size_t n_chunks = chunks_along_dimension(dimension);
    return (n_chunks + shard_size - 1) / shard_size;
}

extern "C" const char*
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
