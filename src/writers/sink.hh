#ifndef H_ACQUIRE_STORAGE_ZARR_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_SINK_V0

#include <concepts>
#include <cstddef>
#include <queue>
#include <string>

namespace acquire::sink::zarr {
struct Sink
{
    virtual ~Sink() noexcept = default;

    virtual bool write(size_t offset,
                       const uint8_t* buf,
                       size_t bytes_of_buf) = 0;

    virtual void close() = 0;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_SINK_V0
