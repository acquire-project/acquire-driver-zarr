#include "zarr.encoder.hh"

#include <filesystem>
#include <thread>

#ifdef min
#undef min
#endif

namespace fs = std::filesystem;

namespace acquire::sink::zarr {

BaseEncoder::BaseEncoder()
  : cursor_{ 0 }
  , bytes_per_pixel_{ 1 }
  , file_handle_{ nullptr }
{
}

void
BaseEncoder::set_bytes_per_pixel(size_t bpp)
{
    bytes_per_pixel_ = bpp;
}

void
BaseEncoder::allocate_buffer(size_t buf_size)
{
    buf_.resize(buf_size);
    cursor_ = 0;
}

size_t
BaseEncoder::write(const uint8_t* beg, const uint8_t* end)
{
    /*
        Some cases:
        1. The buffer already has some data in it.
           => Fill it. If full flush.
        2. The buffer is empty.
           => if (end-beg) > capacity_, just write capacity_ bytes directly.
              Bypass the buffer and avoid a copy.
           => Otherwise append [beg,end) to the buffer

    At the end, flush if the buffer is full and if there are any bytes
                  remaining, try again.
          */

    for (const uint8_t* cur = beg; cur < end;) {
        if (buf_.empty() && (end - cur) >= buf_.size()) {
            cur += write(cur, cur + buf_.size());
        } else {
            // The buffer has some data in it, or we haven't pushed enough
            // data to fill it.
            size_t remaining = buf_.size() - cursor_;
            const uint8_t* fitting_end = std::min(cur + remaining, end);
            std::copy(cur, fitting_end, buf_.data() + cursor_);

            cursor_ += fitting_end - cur;
            cur = fitting_end;
        }

        if (buf_.size() == cursor_) {
            flush();
        }
    }

    return end - beg;
}

size_t
BaseEncoder::flush()
{
    if (0 == cursor_)
        return 0;

    EXPECT(nullptr != file_handle_, "Data on buffer, but no file to flush to.");

    size_t nbytes_out = flush_impl();
    cursor_ = 0;

    return nbytes_out;
}

void
BaseEncoder::set_file(struct file* file_handle)
{
    file_handle_ = file_handle;
}
} // namespace acquire::sink::zarr