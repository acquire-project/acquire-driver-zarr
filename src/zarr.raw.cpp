#include "zarr.raw.hh"
#include "logger.h"

#include <algorithm>

namespace zarr = acquire::sink::zarr;

zarr::RawFile::RawFile(const std::string& filename)
  : last_offset_(0)
  , file_{ 0 }
{
    CHECK(file_create(&file_, filename.c_str(), filename.length()));
}

zarr::RawFile::~RawFile()
{
    file_close(&file_);
}

size_t
zarr::RawFile::write(const uint8_t* beg, const uint8_t* end)
{
    if (end > beg) {
        CHECK(file_write(&file_, last_offset_, beg, end));
        last_offset_ += (end - beg);
    }
    return (end - beg);
}

size_t
zarr::RawFile::flush()
{
    last_offset_ = 0;
    return 0;
}

std::string
acquire::sink::zarr::RawFile::to_json() const
{
    return "null";
}
