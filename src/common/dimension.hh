#ifndef H_ACQUIRE_STORAGE_ZARR_DIMENSION_V0
#define H_ACQUIRE_STORAGE_ZARR_DIMENSION_V0

#include "device/props/storage.h"

#include <string>

namespace acquire::sink::zarr {
struct Dimension
{
  public:
    explicit Dimension(const std::string& name,
                       DimensionType kind,
                       uint32_t array_size_px,
                       uint32_t chunk_size_px,
                       uint32_t shard_size_chunks);
    explicit Dimension(const StorageDimension& dim);

    const std::string name;
    const DimensionType kind;
    const uint32_t array_size_px;
    const uint32_t chunk_size_px;
    const uint32_t shard_size_chunks;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_DIMENSION_V0
