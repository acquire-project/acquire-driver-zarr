#ifndef H_ACQUIRE_ZARR_FILE_WRITER_V0
#define H_ACQUIRE_ZARR_FILE_WRITER_V0

#include "writer.hh"

namespace acquire::sink::zarr {
struct FileWriter
{
    FileWriter() = default;
    ~FileWriter() noexcept = default;

  protected:
    std::vector<std::unique_ptr<struct file>> files_;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_FILE_WRITER_V0
