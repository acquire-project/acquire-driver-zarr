#pragma once

#include "zarr.types.h"

#include <string_view>

struct ZarrDimension_s
{
  public:
    ZarrDimension_s(std::string_view name,
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

    std::string name;       /* Name of the dimension */
    ZarrDimensionType type; /* Type of dimension */

    uint32_t array_size_px;     /* Size of the array along this dimension */
    uint32_t chunk_size_px;     /* Size of a chunk along this dimension */
    uint32_t shard_size_chunks; /* Number of chunks in a shard along this
                                 * dimension */
};