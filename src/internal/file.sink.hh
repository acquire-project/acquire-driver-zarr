#pragma once

#include "sink.hh"

#include <memory>
#include <fstream>
#include <string>

namespace zarr {
struct FileSink : public Sink
{
  public:
    explicit FileSink(const std::string& uri);

    bool write(size_t offset, const uint8_t* data, size_t bytes_of_buf) override;

  private:
    std::unique_ptr<std::ofstream> file_;
};
} // namespace zarr
