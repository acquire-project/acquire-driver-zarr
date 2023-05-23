#include "zarr.encoder.hh"

#include <filesystem>
#include <thread>

#include "blosc.h"

#ifdef min
#undef min
#endif

namespace fs = std::filesystem;

namespace acquire::sink::zarr {

using json = nlohmann::json;

void
to_json(json& j, const BloscCompressor& bc)
{
    j = json{ { "id", std::string(bc.id_) },
              { "cname", bc.codec_id_ },
              { "clevel", bc.clevel_ },
              { "shuffle", bc.shuffle_ } };
}

void
from_json(const json& j, BloscCompressor& bc)
{
    j.at("cname").get_to(bc.codec_id_);
    j.at("clevel").get_to(bc.clevel_);
    j.at("shuffle").get_to(bc.shuffle_);
}

Encoder::Encoder(size_t buffer_size)
  : cursor_{ 0 }
  , bytes_per_pixel_{ 1 }
  , file_handle_{}
  , file_has_been_created_{ false }
{
    buf_.resize(buffer_size);
}

Encoder::~Encoder() noexcept
{
    close_file();
}

void
Encoder::set_bytes_per_pixel(size_t bpp)
{
    bytes_per_pixel_ = bpp;
}

size_t
Encoder::write(const uint8_t* beg, const uint8_t* end)
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
            const size_t nbytes_out = flush();
            //            TRACE("Flushed %lu bytes to disk.", nbytes_out);
        }
    }

    return end - beg;
}

size_t
Encoder::flush()
{
    if (0 == cursor_)
        return 0;

    if (!file_has_been_created_)
        open_file();

    size_t nbytes_out = flush_impl();
    cursor_ = 0;

    return nbytes_out;
}

void
Encoder::set_file_path(const std::string& file_path)
{
    path_ = file_path;
    file_has_been_created_ = false;
}

void
Encoder::open_file()
{
    if (nullptr != file_handle_)
        close_file();

    auto parent_path = fs::path(path_).parent_path();
    if (!fs::is_directory(parent_path))
        fs::create_directories(parent_path);

    file_handle_ = new file;
    CHECK(file_create(file_handle_, path_.c_str(), path_.size()));

    open_file_impl();

    file_has_been_created_ = true;
}

void
Encoder::close_file()
{
    if (!file_has_been_created_ || nullptr == file_handle_)
        return;

    file_close(file_handle_);
    delete file_handle_;
    file_handle_ = nullptr;
    file_has_been_created_ = false;
}

RawEncoder::RawEncoder(size_t bytes_per_tile)
  : Encoder(bytes_per_tile)
  , file_offset_{ 0 }
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

BloscEncoder::BloscEncoder(const BloscCompressor& compressor,
                           size_t bytes_per_chunk)
  : Encoder(bytes_per_chunk)
  , codec_id_{ compressor.codec_id_ }
  , clevel_{ compressor.clevel_ }
  , shuffle_{ compressor.shuffle_ }
{
}

BloscEncoder::~BloscEncoder() noexcept
{
    flush();
    close_file();
}

size_t
BloscEncoder::flush_impl()
{
    auto* buf_c = new uint8_t[cursor_ + BLOSC_MAX_OVERHEAD];
    CHECK(buf_c);

    const auto nbytes_out =
      (size_t)blosc_compress_ctx(clevel_,
                                 shuffle_,
                                 bytes_per_pixel_,
                                 cursor_,
                                 buf_.data(),
                                 buf_c,
                                 cursor_ + BLOSC_MAX_OVERHEAD,
                                 codec_id_.c_str(),
                                 0 /* blocksize - 0:automatic */,
                                 (int)std::thread::hardware_concurrency());

    CHECK(file_write(file_handle_, 0, buf_c, buf_c + nbytes_out));

    delete[] buf_c;
    return nbytes_out;
}

void
BloscEncoder::open_file_impl()
{
}
} // namespace acquire::sink::zarr