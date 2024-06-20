#ifndef H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0

#include "sink.hh"

#include <string>
#include <memory>

struct file;

void
close_file(struct file* file);

namespace acquire::sink::zarr {
struct FileSink : public Sink
{
  public:
    FileSink() = delete;
    explicit FileSink(const std::string& uri);
    FileSink(const FileSink&) = delete;

    FileSink operator=(const FileSink&) = delete;

    bool write(size_t offset, const uint8_t* buf, size_t bytes_of_buf) override;

  private:
    std::unique_ptr<struct file, decltype(&close_file)> file_;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_WRITERS_FILE_SINK_V0
