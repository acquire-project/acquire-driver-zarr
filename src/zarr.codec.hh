#ifndef H_ACQUIRE_ZARR_CODEC_V0
#define H_ACQUIRE_ZARR_CODEC_V0

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "prelude.h"

#ifdef min
#undef min
#endif

struct ImageShape;
struct file;

#ifdef __cplusplus

namespace acquire::sink::zarr {

template<typename W>
concept Writer =
  requires(W writer, const uint8_t* beg, const uint8_t* end, size_t bpp)
{
    // clang-format off
    { writer.write(beg, end) } -> std::convertible_to<size_t>;
    { writer.flush() } -> std::convertible_to<size_t>;
    { writer.to_json() } -> std::convertible_to<std::string>;
    writer.set_bytes_per_pixel(bpp);
    // clang-format on
};

template<Writer W>
struct Buffer
{
    template<typename... Args>
    explicit Buffer(size_t capacity_bytes, Args... args);
    void reserve(size_t capacity_bytes);
    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();
    std::string to_json() const;
    inline void set_bytes_per_pixel(size_t bpp) const {}

  private:
    size_t capacity_;
    std::vector<uint8_t> buf_;
    size_t cursor_;
    W writer_;
};

template<Writer W>
struct Maybe
{
    template<typename... Args>
    void create(Args... args);
    void close();
    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();

    std::string to_json() const;
    inline void set_bytes_per_pixel(size_t bpp) const;

  private:
    std::optional<W> writer_;
};

template<Writer W>
inline size_t
write_all(W& writer, const uint8_t* beg, const uint8_t* end)
{
    if (beg >= end)
        return 0;
    const size_t nbytes = end - beg;
    size_t written = 0;
    do {
        written += writer.write(beg + written, end);
    } while (written < nbytes);
    return nbytes;
}

//
// IMPLEMENTATION
//

template<Writer W>
inline std::string
Buffer<W>::to_json() const
{
    return writer_.to_json();
}

template<Writer W>
inline std::string
Maybe<W>::to_json() const
{
    if (writer_.has_value()) {
        return writer_.value().to_json();
    } else {
        return "null";
    }
}

template<Writer W>
inline void
Maybe<W>::set_bytes_per_pixel(size_t bpp) const
{
    if (writer_.has_value()) {
        writer_.value().set_bytes_per_pixel(bpp);
    }
}

template<Writer W>
template<typename... Args>
inline Buffer<W>::Buffer(size_t capacity_bytes, Args... args)
  : writer_(args...)
  , capacity_(capacity_bytes)
  , cursor_(0)
{
    buf_.reserve(capacity_);
}

template<Writer W>
inline void
Buffer<W>::reserve(size_t capacity_bytes)
{
    buf_.reserve(capacity_bytes);
    capacity_ = capacity_bytes;
}

template<Writer W>
inline size_t
Buffer<W>::write(const uint8_t* beg, const uint8_t* end)
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
        if (buf_.empty() && (end - cur) >= capacity_) {
            cur += write_all(writer_, cur, cur + capacity_);
        } else {
            // The buffer has some data in it, or we haven't pushed enough
            // data to fill it.
            size_t remaining = capacity_ - buf_.size();
            const uint8_t* fitting_end = std::min(cur + remaining, end);
            buf_.insert(buf_.end(), cur, fitting_end);
            cur = fitting_end;
        }

        if (capacity_ == buf_.size()) {
            flush();
        }
    }

    return end - beg;
}

template<Writer W>
inline size_t
Buffer<W>::flush()
{
    const uint8_t* beg = std::to_address(buf_.begin());
    const uint8_t* end = std::to_address(buf_.end());
    const size_t written = write_all(writer_, beg, end);
    buf_.clear();
    return written;
}

template<Writer W>
template<typename... Args>
inline void
Maybe<W>::create(Args... args)
{
    EXPECT(!writer_.has_value(), "Attempted to open an already open writer.");
    writer_.emplace(args...);
}

/// @brief Flush and close the wrapped writer.
/// @tparam W Writer
template<Writer W>
inline void
Maybe<W>::close()
{
    if (writer_.has_value()) {
        writer_.value().flush();
        writer_.reset();
    }
}

template<Writer W>
inline size_t
Maybe<W>::write(const uint8_t* beg, const uint8_t* end)
{
    EXPECT(writer_.has_value(), "Expected writer to be open.");
    return writer_.value().write(beg, end);
}

template<Writer W>
inline size_t
Maybe<W>::flush()
{
    if (writer_.has_value()) {
        return writer_.value().flush();
    }
    LOG("Flushing a closed writer.");
    return 0;
}

} // end namespace
#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_CODEC_V0
