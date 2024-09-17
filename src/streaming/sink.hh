#pragma once

#include <cstdint> // uint8_t
#include <cstddef> // size_t

namespace zarr {
class Sink
{
  public:
    virtual ~Sink() = default;

    /**
     * @brief Write data to the sink.
     * @param offset The offset in the sink to write to.
     * @param buf The buffer to write to the sink.
     * @param bytes_of_buf The number of bytes to write from @p buf.
     * @return True if the write was successful, false otherwise.
     * @throws std::runtime_error if @p buf is nullptr or the write fails.
     */
    [[nodiscard]] virtual bool write(size_t offset,
                                     const uint8_t* buf,
                                     size_t bytes_of_buf) = 0;
};
} // namespace zarr
