#include "dimension.hh"

#include <algorithm>

namespace zarr = acquire::sink::zarr;

namespace {
inline std::string
trim(const std::string& s)
{
    // trim left
    std::string trimmed = s;
    trimmed.erase(trimmed.begin(),
                  std::find_if(trimmed.begin(), trimmed.end(), [](char c) {
                      return !std::isspace(c);
                  }));

    // trim right
    trimmed.erase(std::find_if(trimmed.rbegin(),
                               trimmed.rend(),
                               [](char c) { return !std::isspace(c); })
                    .base(),
                  trimmed.end());

    return trimmed;
}
} // namespace

zarr::Dimension::Dimension(const std::string& name,
                           DimensionType kind,
                           uint32_t array_size_px,
                           uint32_t chunk_size_px,
                           uint32_t shard_size_chunks)
  : name{ trim(name) }
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

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif // _WIN32

extern "C" acquire_export int
unit_test__trim()
{
    try {
        EXPECT(trim("  foo") == "foo", "Failed to trim left.");
        EXPECT(trim("foo  ") == "foo", "Failed to trim right.");
        EXPECT(trim("  foo  ") == "foo", "Failed to trim both.");
        EXPECT(trim("foo") == "foo", "Failed to trim either.");
        EXPECT(trim("").empty(), "Failed to trim empty.");
        return 1;
    } catch (const std::exception& e) {
        LOGE("Exception: %s", e.what());
    } catch (...) {
        LOGE("Unknown exception");
    }

    return 0;
}

#endif // NO_UNIT_TESTS
