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

template<typename SinkCreatorT>
concept SinkCreator = requires(SinkCreatorT sink_creator,
                               const std::string& base_uri,
                               std::vector<Dimension> dimensions,
                               const std::vector<std::string>& paths,
                               std::vector<Sink*>& sinks) {
    {
        sink_creator.create_chunk_sinks(base_uri, dimensions, sinks)
    } -> std::convertible_to<bool>;
    {
        sink_creator.create_shard_sinks(base_uri, dimensions, sinks)
    } -> std::convertible_to<bool>;
    {
        sink_creator.create_metadata_sinks(paths, sinks)
    } -> std::convertible_to<bool>;
};

template<SinkType T>
Sink*
sink_open(const std::string& uri)
{
    return (Sink*)new T(uri);
}

template<SinkType SinkT>
void
sink_close(Sink* sink);

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_SINK_V0
