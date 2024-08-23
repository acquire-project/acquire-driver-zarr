#pragma once

#include "../internal/macros.hh"
#include "device/props/storage.h"

#include <string>

namespace acquire::sink::zarr {
struct Dimension
{
  public:
    explicit Dimension(const std::string& name,
                       DimensionType kind,
                       unsigned int array_size_px,
                       unsigned int chunk_size_px,
                       unsigned int shard_size_chunks);

    explicit Dimension(const StorageDimension& dim);

    const std::string name;
    const DimensionType kind;
    const unsigned int array_size_px;
    const unsigned int chunk_size_px;
    const unsigned int shard_size_chunks;
};
} // namespace acquire::sink::zarr
