#pragma once

#include "sink.hh"

#include <memory>
#include <fstream>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    explicit FileSink(std::string_view filename);

    bool write(size_t offset, const uint8_t* data, size_t bytes_of_buf) override;

  private:
    std::ofstream file_;
};
} // namespace zarr
