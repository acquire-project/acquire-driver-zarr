#include "file.sink.hh"
#include "logger.hh"

#include <filesystem>
#include <latch>

namespace fs = std::filesystem;

zarr::FileSink::FileSink(const std::string& uri)
  : file_(nullptr)
{
    if (fs::exists(uri)) {
        LOG_WARNING("File already exists: %s. Truncating.", uri.c_str());
        return;
    }
    file_ = std::make_unique<std::ofstream>(uri, std::ios::binary);
}

bool
zarr::FileSink::write(size_t offset, const uint8_t* data, size_t bytes_of_buf)
{
    EXPECT(data, "Null pointer: data");
    if (bytes_of_buf == 0)
        return true;

    file_->write(reinterpret_cast<const char*>(data), bytes_of_buf);
    return true;
}
