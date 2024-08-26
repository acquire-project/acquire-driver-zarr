#pragma once

#include "sink.hh"

#include <memory>
#include <fstream>
#include <string>

namespace zarr {
struct FileSink : public Sink
{
  public:
    explicit FileSink(const std::string& filename);

    bool write(size_t offset, const uint8_t* data, size_t bytes_of_buf) override;

  private:
    std::ofstream file_;
};
} // namespace zarr
