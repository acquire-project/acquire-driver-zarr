#pragma once

#include "zarr.types.h"

#include <string>
#include <string_view>
#include <vector>

struct ZarrDimension
{
    ZarrDimension() = default;
    ZarrDimension(std::string_view name,
                  ZarrDimensionType type,
                  uint32_t array_size_px,
                  uint32_t chunk_size_px,
                  uint32_t shard_size_chunks)
      : name(name)
      , type(type)
      , array_size_px(array_size_px)
      , chunk_size_px(chunk_size_px)
      , shard_size_chunks(shard_size_chunks)
    {
    }

    std::string name;
    ZarrDimensionType type{ ZarrDimensionType_Space };

    uint32_t array_size_px{ 0 };
    uint32_t chunk_size_px{ 0 };
    uint32_t shard_size_chunks{ 0 };
};

class ArrayDimensions
{
  public:
    ArrayDimensions(std::vector<ZarrDimension>&& dims, ZarrDataType dtype);

    size_t ndims() const;

    const ZarrDimension& operator[](size_t idx) const;
    const ZarrDimension& at(size_t idx) const { return operator[](idx); }

    const ZarrDimension& final_dim() const;
    const ZarrDimension& height_dim() const;
    const ZarrDimension& width_dim() const;

    /**
     * @brief Get the index of a chunk in the chunk lattice for a given frame
     * and dimension.
     * @param frame_id The frame ID.
     * @param dimension_idx The index of the dimension in the dimension vector.
     * @return The index of the chunk in the chunk lattice.
     */
    uint32_t chunk_lattice_index(uint64_t frame_id, uint32_t dim_index) const;

    /**
     * @brief Find the offset in the array of chunk buffers for the given frame.
     * @param frame_id The frame ID.
     * @return The offset in the array of chunk buffers.
     */
    uint32_t tile_group_offset(uint64_t frame_id) const;

    /**
     * @brief Find the byte offset inside a chunk for a given frame and data
     * type.
     * @param frame_id The frame ID.
     * @param dims The dimensions of the array.
     * @param type The data type of the array.
     * @return The byte offset inside a chunk.
     */
    uint64_t chunk_internal_offset(uint64_t frame_id) const;

    /**
     * @brief Get the number of chunks to hold in memory.
     * @return The number of chunks to buffer before writing out.
     */
    uint32_t number_of_chunks_in_memory() const;

    /**
     * @brief Get the size, in bytes, of a single raw chunk.
     * @return The number of bytes to allocate for a chunk.
     */
    size_t bytes_per_chunk() const;

    /**
     * @brief Get the number of shards to write at one time.
     * @return The number of shards to buffer and write out.
     */
    uint32_t number_of_shards() const;

    /**
     * @brief Get the number of chunks in a single shard.
     * @return The number of chunks in a shard.
     */
    uint32_t chunks_per_shard() const;

    /**
     * @brief Get the shard index for a given chunk index, given array dimensions.
     * @param chunk_index The index of the chunk.
     * @return The index of the shard containing the chunk.
     */
    uint32_t shard_index_for_chunk(uint32_t chunk_index) const;

    /**
     * @brief Get the streaming index of a chunk within a shard.
     * @param chunk_index The index of the chunk.
     * @return The index of the chunk within the shard.
     */
    uint32_t shard_internal_index(uint32_t chunk_index) const;

  private:
    std::vector<ZarrDimension> dims_;
    ZarrDataType dtype_;
};