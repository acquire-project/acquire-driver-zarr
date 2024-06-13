#ifndef H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0

#include "sink.hh"
#include "platform.h"

namespace acquire::sink::zarr {
struct FileSink : public Sink
{
  public:
    explicit FileSink(const std::string& uri);
    ~FileSink() override;

    [[nodiscard]] bool write(size_t offset,
                             const uint8_t* buf,
                             size_t bytes_of_buf) override;
    void close() override;

  private:
    struct file* file_;

    void close_file_() noexcept;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0
