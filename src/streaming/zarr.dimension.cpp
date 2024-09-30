#include "zarr.dimension.hh"
#include "macros.hh"
#include "zarr.common.hh"

ArrayDimensions::ArrayDimensions(std::vector<ZarrDimension>&& dims,
                                 ZarrDataType dtype)
  : dims_(std::move(dims))
  , dtype_(dtype)
{
    EXPECT(dims_.size() > 2, "Array must have at least three dimensions.");
}

size_t
ArrayDimensions::ndims() const
{
    return dims_.size();
}

const ZarrDimension&
ArrayDimensions::operator[](size_t idx) const
{
    return dims_[idx];
}

const ZarrDimension&
ArrayDimensions::final_dim() const
{
    return dims_[0];
}

const ZarrDimension&
ArrayDimensions::height_dim() const
{
    return dims_[ndims() - 2];
}

const ZarrDimension&
ArrayDimensions::width_dim() const
{
    return dims_.back();
}

uint32_t
ArrayDimensions::chunk_lattice_index(uint64_t frame_id,
                                     uint32_t dim_index) const
{
    // the last two dimensions are special cases
    EXPECT(dim_index < ndims() - 2, "Invalid dimension index: ", dim_index);

    // the first dimension is a special case
    if (dim_index == 0) {
        auto divisor = dims_.front().chunk_size_px;
        for (auto i = 1; i < ndims() - 2; ++i) {
            const auto& dim = dims_[i];
            divisor *= dim.array_size_px;
        }

        CHECK(divisor);
        return frame_id / divisor;
    }

    size_t mod_divisor = 1, div_divisor = 1;
    for (auto i = dim_index; i < ndims() - 2; ++i) {
        const auto& dim = dims_[i];
        mod_divisor *= dim.array_size_px;
        div_divisor *= (i == dim_index ? dim.chunk_size_px : dim.array_size_px);
    }

    CHECK(mod_divisor);
    CHECK(div_divisor);

    return (frame_id % mod_divisor) / div_divisor;
}

uint32_t
ArrayDimensions::tile_group_offset(uint64_t frame_id) const
{
    std::vector<size_t> strides(dims_.size(), 1);
    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        const auto a = dim.array_size_px, c = dim.chunk_size_px;
        strides[i - 1] = strides[i] * ((a + c - 1) / c);
    }

    size_t offset = 0;
    for (auto i = ndims() - 3; i > 0; --i) {
        const auto idx = chunk_lattice_index(frame_id, i);
        const auto stride = strides[i];
        offset += idx * stride;
    }

    return offset;
}

uint64_t
ArrayDimensions::chunk_internal_offset(uint64_t frame_id) const
{
    const auto tile_size = zarr::bytes_of_type(dtype_) *
                           width_dim().chunk_size_px *
                           height_dim().chunk_size_px;

    uint64_t offset = 0;
    std::vector<uint64_t> array_strides(ndims() - 2, 1),
      chunk_strides(ndims() - 2, 1);

    for (auto i = (int)ndims() - 3; i > 0; --i) {
        const auto& dim = dims_[i];
        const auto internal_idx =
          (frame_id / array_strides[i]) % dim.array_size_px % dim.chunk_size_px;

        array_strides[i - 1] = array_strides[i] * dim.array_size_px;
        chunk_strides[i - 1] = chunk_strides[i] * dim.chunk_size_px;
        offset += internal_idx * chunk_strides[i];
    }

    // final dimension
    {
        const auto& dim = dims_[0];
        const auto internal_idx =
          (frame_id / array_strides.front()) % dim.chunk_size_px;
        offset += internal_idx * chunk_strides.front();
    }

    return offset * tile_size;
}

uint32_t
ArrayDimensions::number_of_chunks_in_memory() const
{
    uint32_t n_chunks = 1;
    for (auto i = 1; i < ndims(); ++i) {
        n_chunks *= zarr::chunks_along_dimension(dims_[i]);
    }

    return n_chunks;
}

size_t
ArrayDimensions::bytes_per_chunk() const
{
    auto n_bytes = zarr::bytes_of_type(dtype_);
    for (const auto& d : dims_) {
        n_bytes *= d.chunk_size_px;
    }

    return n_bytes;
}

uint32_t
ArrayDimensions::number_of_shards() const
{
    size_t n_shards = 1;
    for (auto i = 1; i < ndims(); ++i) {
        const auto& dim = dims_[i];
        n_shards *= zarr::shards_along_dimension(dim);
    }

    return n_shards;
}

uint32_t
ArrayDimensions::chunks_per_shard() const
{
    size_t n_chunks = 1;
    for (const auto& dim : dims_) {
        n_chunks *= dim.shard_size_chunks;
    }

    return n_chunks;
}

uint32_t
ArrayDimensions::shard_index_for_chunk(uint32_t chunk_index) const
{
    // make chunk strides
    std::vector<uint64_t> chunk_strides;
    chunk_strides.resize(dims_.size());
    chunk_strides.back() = 1;

    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_strides[i - 1] =
          chunk_strides[i] * zarr::chunks_along_dimension(dim);
    }

    // get chunk indices
    std::vector<uint32_t> chunk_lattice_indices(ndims());
    for (auto i = ndims() - 1; i > 0; --i) {
        chunk_lattice_indices[i] =
          chunk_index % chunk_strides[i - 1] / chunk_strides[i];
    }

    // make shard strides
    std::vector<uint32_t> shard_strides(ndims(), 1);
    for (auto i = ndims() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        shard_strides[i-1] = shard_strides[i] * zarr::shards_along_dimension(dim);
    }

    std::vector<uint32_t> shard_lattice_indices;
    for (auto i = 0; i < ndims(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dims_[i].shard_size_chunks);
    }

    uint32_t index = 0;
    for (auto i = 0; i < ndims(); ++i) {
        index += shard_lattice_indices[i] * shard_strides[i];
    }

    return index;
}

uint32_t
ArrayDimensions::shard_internal_index(uint32_t chunk_index) const
{
    // make chunk strides
    std::vector<uint64_t> chunk_strides;
    chunk_strides.resize(dims_.size());
    chunk_strides.back() = 1;

    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_strides[i - 1] =
          chunk_strides[i] * zarr::chunks_along_dimension(dim);
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices(ndims());
    for (auto i = ndims() - 1; i > 0; --i) {
        chunk_lattice_indices[i] =
          chunk_index % chunk_strides[i - 1] / chunk_strides[i];
    }
    chunk_lattice_indices[0] = chunk_index / chunk_strides.front();

    // make shard lattice indices
    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < ndims(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dims_[i].shard_size_chunks);
    }

    std::vector<size_t> chunk_internal_strides(ndims(), 1);
    for (auto i = ndims() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_internal_strides[i - 1] =
          chunk_internal_strides[i] * dim.shard_size_chunks;
    }

    size_t index = 0;

    for (auto i = 0; i < ndims(); ++i) {
        index += (chunk_lattice_indices[i] % dims_[i].shard_size_chunks) *
                 chunk_internal_strides[i];
    }

    return index;
}