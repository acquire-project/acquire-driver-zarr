#include "file.sink.hh"
#include "logger.hh"

#include <filesystem>
#include <latch>

namespace fs = std::filesystem;

zarr::FileSink::FileSink(std::string_view filename)
: file_(filename.data(), std::ios::binary | std::ios::trunc)
{
}

bool
zarr::FileSink::write(size_t offset, const uint8_t* data, size_t bytes_of_buf)
{
    EXPECT(data, "Null pointer: data");
    if (bytes_of_buf == 0) {
        return true;
    }

    file_.write(reinterpret_cast<const char*>(data), bytes_of_buf);
    return true;
}
