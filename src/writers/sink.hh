#ifndef H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_V0

#include <cstdint> // uint8_t

namespace acquire::sink::zarr {
struct Sink
{
    virtual ~Sink() noexcept = default;

    /// @brief Write data to the sink.
    /// @param offset The offset in the sink to write to.
    /// @param buf The buffer to write to the sink.
    /// @param bytes_of_buf The number of bytes to write from @p buf.
    /// @return True if the write was successful, false otherwise.
    [[nodiscard]] virtual bool write(size_t offset,
                                     const uint8_t* buf,
                                     size_t bytes_of_buf) = 0;
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_V0
