#ifndef H_ACQUIRE_STORAGE_ZARR_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_SINK_V0

#include <concepts>
#include <cstddef>
#include <queue>
#include <string>

#include "../common.hh"

namespace acquire::sink::zarr {
struct Sink
{
    virtual ~Sink() noexcept = default;

    virtual bool write(size_t offset,
                       const uint8_t* buf,
                       size_t bytes_of_buf) = 0;
};

template<typename SinkT>
concept SinkType =
  requires(SinkT sink, size_t offset, const uint8_t* buf, size_t bytes_of_buf) {
      { sink.write(offset, buf, bytes_of_buf) } -> std::convertible_to<bool>;
  };

template<SinkType SinkT>
void
sink_close(Sink* sink);

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_SINK_V0
