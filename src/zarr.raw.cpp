#include "zarr.raw.hh"

namespace acquire::sink::zarr {
RawEncoder::RawEncoder()
  : file_offset_{ 0 }
{
}

void
  RawEncoder::set_file(struct file* file_handle)
{
    BaseEncoder::set_file(file_handle);
    file_offset_ = 0;
}

size_t
RawEncoder::flush_impl()
{
    CHECK(file_write(
      file_handle_, file_offset_, buf_.data(), buf_.data() + cursor_));
    file_offset_ += cursor_;

    return cursor_;
}
} // namespace acquire::sink::zarr