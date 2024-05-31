#ifndef H_ACQUIRE_ZARR_ARRAY_V0
#define H_ACQUIRE_ZARR_ARRAY_V0

#include <string>

namespace acquire::sink::zarr {
struct ZarrArray
{
  public:
    ZarrArray() = default;
    ~ZarrArray() noexcept = default;

    virtual std::string make_array_metadata() const = 0;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_ARRAY_V0
