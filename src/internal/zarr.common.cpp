#include "zarr.common.hh"
#include "zarr_errors.h"
#include "logger.hh"

#include <stdexcept>

size_t
zarr::bytes_of_type(ZarrDataType data_type)
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
zarr::bytes_of_frame(const std::vector<Dimension>& dims, ZarrDataType type)
{
    return bytes_of_type(type) * dims.back().array_size_px *
           dims[dims.size() - 2].array_size_px;
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
    if (shard_size == 0) {
        return 0;
    }

    const size_t n_chunks = chunks_along_dimension(dimension);
    return (n_chunks + shard_size - 1) / shard_size;
}

size_t
zarr::chunk_lattice_index(size_t frame_id,
                          size_t dimension_idx,
                          const std::vector<Dimension>& dims)
{
    // the last two dimensions are special cases
    EXPECT(dimension_idx < dims.size() - 2,
           "Invalid dimension index: %zu",
           dimension_idx);

    // the first dimension is a special case
    if (dimension_idx == 0) {
        size_t divisor = dims.front().chunk_size_px;
        for (auto i = 1; i < dims.size() - 2; ++i) {
            const auto& dim = dims[i];
            divisor *= dim.array_size_px;
        }

        CHECK(divisor);
        return frame_id / divisor;
    }

    size_t mod_divisor = 1, div_divisor = 1;
    for (auto i = dimension_idx; i < dims.size() - 2; ++i) {
        const auto& dim = dims[i];
        mod_divisor *= dim.array_size_px;
        div_divisor *=
          (i == dimension_idx ? dim.chunk_size_px : dim.array_size_px);
    }

    CHECK(mod_divisor);
    CHECK(div_divisor);

    return (frame_id % mod_divisor) / div_divisor;
}

size_t
zarr::tile_group_offset(size_t frame_id, const std::vector<Dimension>& dims)
{
    std::vector<size_t> strides;
    strides.push_back(1);
    for (auto i = dims.size() - 1; i > 0; --i) {
        const auto& dim = dims[i];
        CHECK(dim.chunk_size_px);
        const auto a = dim.array_size_px, c = dim.chunk_size_px;
        strides.insert(strides.begin(), strides.front() * ((a + c - 1) / c));
    }

    size_t offset = 0;
    for (auto i = dims.size() - 3; i > 0; --i) {
        const auto idx = chunk_lattice_index(frame_id, i, dims);
        const auto stride = strides[i];
        offset += idx * stride;
    }

    return offset;
}

size_t
zarr::chunk_internal_offset(size_t frame_id,
                            const std::vector<Dimension>& dims,
                            ZarrDataType type)
{
    const Dimension& x_dim = dims.back();
    const Dimension& y_dim = dims[dims.size() - 2];
    const auto tile_size =
      bytes_of_type(type) * x_dim.chunk_size_px * y_dim.chunk_size_px;

    size_t offset = 0;
    std::vector<size_t> array_strides, chunk_strides;
    array_strides.push_back(1);
    chunk_strides.push_back(1);

    for (auto i = (int)dims.size() - 3; i >= 0; --i) {
        const auto& dim = dims[i];

        if (i > 0) {
            CHECK(dim.array_size_px);
        }

        CHECK(dim.chunk_size_px);
        CHECK(array_strides.front());

        const auto internal_idx =
          i == 0 ? (frame_id / array_strides.front()) % dim.chunk_size_px
                 : (frame_id / array_strides.front()) % dim.array_size_px %
                     dim.chunk_size_px;
        offset += internal_idx * chunk_strides.front();

        array_strides.insert(array_strides.begin(),
                             array_strides.front() * dim.array_size_px);
        chunk_strides.insert(chunk_strides.begin(),
                             chunk_strides.front() * dim.chunk_size_px);
    }

    return offset * tile_size;
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

size_t
zarr::number_of_chunks_in_memory(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (auto i = 1; i < dimensions.size(); ++i) {
        n_chunks *= chunks_along_dimension(dimensions[i]);
    }

    return n_chunks;
}

size_t
zarr::bytes_per_chunk(const std::vector<Dimension>& dimensions,
                      ZarrDataType type)
{
    auto n_bytes = bytes_of_type(type);
    for (const auto& d : dimensions) {
        n_bytes *= d.chunk_size_px;
    }

    return n_bytes;
}

size_t
zarr::number_of_shards(const std::vector<Dimension>& dimensions)
{
    size_t n_shards = 1;
    for (auto i = 1; i < dimensions.size(); ++i) {
        const auto& dim = dimensions[i];
        n_shards *= shards_along_dimension(dim);
    }

    return n_shards;
}

size_t
zarr::chunks_per_shard(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (const auto& dim : dimensions) {
        n_chunks *= dim.shard_size_chunks;
    }

    return n_chunks;
}

size_t
zarr::shard_index_for_chunk(size_t chunk_index,
                            const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        const auto& dim = dimensions[i];
        chunk_strides.insert(chunk_strides.begin(),
                             chunk_strides.front() *
                               chunks_along_dimension(dim));
        CHECK(chunk_strides.front());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        chunk_lattice_indices.insert(chunk_lattice_indices.begin(),
                                     chunk_index % chunk_strides[i - 1] /
                                       chunk_strides[i]);
    }
    chunk_lattice_indices.insert(chunk_lattice_indices.begin(),
                                 chunk_index / chunk_strides.front());

    // make shard strides
    std::vector<size_t> shard_strides(1, 1);
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        const auto& dim = dimensions[i];
        shard_strides.insert(shard_strides.begin(),
                             shard_strides.front() *
                               shards_along_dimension(dim));
        CHECK(shard_strides.front());
    }

    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dimensions[i].shard_size_chunks);
    }

    size_t index = 0;
    for (auto i = 0; i < dimensions.size(); ++i) {
        index += shard_lattice_indices[i] * shard_strides[i];
    }

    return index;
}

size_t
zarr::shard_internal_index(size_t chunk_idx,
                           const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        const auto& dim = dimensions[i];
        chunk_strides.insert(chunk_strides.begin(),
                             chunk_strides.front() *
                               chunks_along_dimension(dim));
        CHECK(chunk_strides.front());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        chunk_lattice_indices.insert(chunk_lattice_indices.begin(),
                                     chunk_idx % chunk_strides.at(i - 1) /
                                       chunk_strides[i]);
    }
    chunk_lattice_indices.insert(chunk_lattice_indices.begin(),
                                 chunk_idx / chunk_strides.front());

    // make shard lattice indices
    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dimensions[i].shard_size_chunks);
    }

    std::vector<size_t> chunk_internal_strides(1, 1);
    for (auto i = dimensions.size() - 1; i > 0; --i) {
        const auto& dim = dimensions[i];
        chunk_internal_strides.insert(chunk_internal_strides.begin(),
                                      chunk_internal_strides.front() *
                                        dim.shard_size_chunks);
    }

    size_t index = 0;

    for (auto i = 0; i < dimensions.size(); ++i) {
        index += (chunk_lattice_indices[i] % dimensions[i].shard_size_chunks) *
                 chunk_internal_strides[i];
    }

    return index;
}

const char*
zarr::compression_codec_to_string(ZarrCompressionCodec codec)
{
    switch (codec) {
        case ZarrCompressionCodec_None:
            return "none";
        case ZarrCompressionCodec_BloscLZ4:
            return "blosc-lz4";
        case ZarrCompressionCodec_BloscZstd:
            return "blosc-zstd";
        default:
            return "(unknown)";
    }
}
