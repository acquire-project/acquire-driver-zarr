#include "utilities.hh"
#include "zarr.hh"

#include "platform.h"
#include "thread.pool.hh"

#include <cmath>
#include <thread>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

size_t
common::chunks_along_dimension(const Dimension& dimension)
{
    EXPECT(dimension.chunk_size_px > 0, "Invalid chunk_size size.");

    return (dimension.array_size_px + dimension.chunk_size_px - 1) /
           dimension.chunk_size_px;
}

size_t
common::shards_along_dimension(const Dimension& dimension)
{
    const size_t shard_size = dimension.shard_size_chunks;
    if (shard_size == 0) {
        return 0;
    }

    const size_t n_chunks = chunks_along_dimension(dimension);
    return (n_chunks + shard_size - 1) / shard_size;
}

size_t
common::number_of_chunks_in_memory(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        n_chunks *= chunks_along_dimension(dimensions[i]);
    }

    return n_chunks;
}

size_t
common::number_of_shards(const std::vector<Dimension>& dimensions)
{
    size_t n_shards = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        n_shards *= shards_along_dimension(dim);
    }

    return n_shards;
}

size_t
common::chunks_per_shard(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (const auto& dim : dimensions) {
        n_chunks *= dim.shard_size_chunks;
    }

    return n_chunks;
}

size_t
common::bytes_per_chunk(const std::vector<Dimension>& dimensions,
                        const SampleType& type)
{
    auto n_bytes = bytes_of_type(type);
    for (const auto& d : dimensions) {
        n_bytes *= d.chunk_size_px;
    }

    return n_bytes;
}

const char*
common::sample_type_to_dtype(SampleType t)

{
    static const char* table[] = { "u1", "u2", "i1", "i2",
                                   "f4", "u2", "u2", "u2" };
    if (t < countof(table)) {
        return table[t];
    } else {
        throw std::runtime_error("Invalid sample type.");
    }
}

const char*
common::sample_type_to_string(SampleType t) noexcept
{
    static const char* table[] = { "u8",  "u16", "i8",  "i16",
                                   "f32", "u16", "u16", "u16" };
    if (t < countof(table)) {
        return table[t];
    } else {
        return "unrecognized pixel type";
    }
}

size_t
common::align_up(size_t n, size_t align)
{
    EXPECT(align > 0, "Alignment must be greater than zero.");
    return align * ((n + align - 1) / align);
}
