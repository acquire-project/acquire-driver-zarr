#include "zarr.common.hh"
#include "zarr_errors.h"
#include "logger.hh"

#include <stdexcept>

size_t
zarr::bytes_of_data_type(ZarrDataType data_type)
{
    switch (data_type) {
        case ZarrDataType_int8:
        case ZarrDataType_uint8:
            return 1;
        case ZarrDataType_int16:
        case ZarrDataType_uint16:
        case ZarrDataType_float16:
            return 2;
        case ZarrDataType_int32:
        case ZarrDataType_uint32:
        case ZarrDataType_float32:
            return 4;
        case ZarrDataType_int64:
        case ZarrDataType_uint64:
        case ZarrDataType_float64:
            return 8;
        default:
            throw std::runtime_error("Invalid data type.");
    }
}

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

size_t
zarr::chunk_lattice_index(size_t frame_id,
                          size_t dimension_idx,
                          const std::vector<zarr::Dimension>& dims)
{
    // the last two dimensions are special cases
    EXPECT(dimension_idx < dims.size() - 2,
           "Invalid dimension index: %zu",
           dimension_idx);

    // the first dimension is a special case
    if (dimension_idx == 0) {
        size_t divisor = dims.front().chunk_size_px;
        for (auto i = 1; i < dims.size() - 2; ++i) {
            const auto& dim = dims.at(i);
            divisor *= dim.array_size_px;
        }

        CHECK(divisor);
        return frame_id / divisor;
    }

    size_t mod_divisor = 1, div_divisor = 1;
    for (auto i = dimension_idx; i < dims.size() - 2; ++i) {
        const auto& dim = dims.at(i);
        mod_divisor *= dim.array_size_px;
        div_divisor *=
          (i == dimension_idx ? dim.chunk_size_px : dim.array_size_px);
    }

    CHECK(mod_divisor);
    CHECK(div_divisor);

    return (frame_id % mod_divisor) / div_divisor;
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
