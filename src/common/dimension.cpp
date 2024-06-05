#include "dimension.hh"
#include "utilities.hh"

namespace zarr = acquire::sink::zarr;

zarr::Dimension::Dimension(const std::string& name,
                           DimensionType kind,
                           uint32_t array_size_px,
                           uint32_t chunk_size_px,
                           uint32_t shard_size_chunks)
  : name{ name }
  , kind{ kind }
  , array_size_px{ array_size_px }
  , chunk_size_px{ chunk_size_px }
  , shard_size_chunks{ shard_size_chunks }
{
    EXPECT(kind < DimensionTypeCount, "Invalid dimension type.");
    EXPECT(!name.empty(), "Dimension name cannot be empty.");
}

zarr::Dimension::Dimension(const StorageDimension& dim)
  : Dimension(dim.name.str,
              dim.kind,
              dim.array_size_px,
              dim.chunk_size_px,
              dim.shard_size_chunks)
{
}