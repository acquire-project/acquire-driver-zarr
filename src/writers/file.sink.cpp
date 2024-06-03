#include "file.sink.hh"
#include "../common/thread.pool.hh"

#include <latch>

namespace zarr = acquire::sink::zarr;

zarr::FileSink::FileSink(const std::string& uri)
  : file_{ new struct file }
{
    CHECK(file_create(file_, uri.c_str(), uri.size() + 1));
}

zarr::FileSink::~FileSink()
{
    close_file_();
}

bool
zarr::FileSink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    if (!file_) {
        return false;
    }

    return file_write(file_, offset, buf, buf + bytes_of_buf);
}

void
zarr::FileSink::close()
{
    close_file_();
}

void
zarr::FileSink::close_file_() noexcept
{
    if (file_) {
        try {
            file_close(file_);
        } catch (const std::exception& exc) {
            LOGE("Failed to close file: %s", exc.what());
        } catch (...) {
            LOGE("Failed to close file: (unknown)");
        }
        file_ = nullptr;
    }
}
