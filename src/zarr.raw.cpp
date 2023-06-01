#include "zarr.raw.hh"

namespace acquire::sink::zarr {
RawEncoder::RawEncoder()
  : file_offset_{ 0 }
{
}

RawEncoder::~RawEncoder() noexcept
{
    flush();
    close_file();
}

size_t
RawEncoder::flush_impl()
{
    CHECK(file_write(
      file_handle_, file_offset_, buf_.data(), buf_.data() + cursor_));
    file_offset_ += cursor_;

    return cursor_;
}

void
RawEncoder::open_file_impl()
{
    file_offset_ = 0;
}
} // namespace acquire::sink::zarr