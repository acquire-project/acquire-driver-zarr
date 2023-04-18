#ifndef H_ACQUIRE_STORAGE_ZARR_BLOSC_V0
#define H_ACQUIRE_STORAGE_ZARR_BLOSC_V0

struct ImageShape;

#ifdef __cplusplus

#include "zarr.codec.hh"
#include <vector>
#include <thread>
#include <blosc.h>

namespace acquire::sink::zarr {

enum class CodecId
{
    Zstd,
    Lz4,
};

template<CodecId C>
constexpr const char*
compression_codec_as_string();

template<>
constexpr const char*
compression_codec_as_string<CodecId::Zstd>()
{
    return "zstd";
}

template<>
constexpr const char*
compression_codec_as_string<CodecId::Lz4>()
{
    return "lz4";
}

template<Writer W, CodecId C>
struct BloscEncoder
{
    template<typename... WriterArgs>
    explicit BloscEncoder(WriterArgs... writer_args);

    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();

    ///< Compression parameters
    static constexpr char id_[] = "blosc";
    static constexpr int clevel_ = 1;
    static constexpr int shuffle_ = 1;
    static constexpr const char* compressor_name_ =
      compression_codec_as_string<C>();

    std::string to_json() const;
    void set_bytes_per_pixel(size_t bpp);

  private:
    W writer_;
    std::vector<uint8_t> buf_;
    size_t bytes_per_pixel_;
};

template<Writer W, CodecId C>
inline std::string
BloscEncoder<W, C>::to_json() const
{
    const auto fmt = R"({"id":"%s",)"
                     R"("cname":"%s",)"
                     R"("clevel":%d,)"
                     R"("shuffle":%d})";
    char buf[256] = { 0 };
    CHECK(0 <
          snprintf(
            buf, sizeof(buf), fmt, id_, compressor_name_, clevel_, shuffle_));

    return { buf };
}

template<Writer W, CodecId C>
template<typename... WriterArgs>
inline BloscEncoder<W, C>::BloscEncoder(WriterArgs... writer_args)
  : writer_(writer_args...)
  , bytes_per_pixel_(1)
{
}

template<Writer W, CodecId C>
inline void
BloscEncoder<W, C>::set_bytes_per_pixel(size_t bpp)
{
    bytes_per_pixel_ = bpp;
}

/// Compresses bytes from `[beg,end)` using blosc, then writes those to the
/// wrapped writer and flushes.
/// @return The number of bytes consumed from the `[beg,end)` interval.
template<Writer W, CodecId C>
inline size_t
BloscEncoder<W, C>::write(const uint8_t* beg, const uint8_t* end)
{
    if (beg >= end)
        return 0;
    const size_t nbytes_in = end - beg;
    buf_.resize(nbytes_in + BLOSC_MAX_OVERHEAD);

    int nbytes_out = blosc_compress_ctx(clevel_,
                                        shuffle_,
                                        bytes_per_pixel_,
                                        nbytes_in,
                                        beg,
                                        &buf_[0],
                                        nbytes_in + BLOSC_MAX_OVERHEAD,
                                        compressor_name_,
                                        0 /* blocksize - 0:automatic */,
                                        std::thread::hardware_concurrency());
    EXPECT(nbytes_out >= 0, "blosc_compress_ctx failed.");

    TRACE("nbytes: %llu, cbytes: %llu (ratio: %0.3f)",
          (uint64_t)nbytes_in,
          (uint64_t)nbytes_out,
          (float)nbytes_in / (float)nbytes_out);

    write_all(writer_, &buf_[0], &buf_[nbytes_out]);
    flush();

    return nbytes_in;
}

template<Writer W, CodecId C>
inline size_t
BloscEncoder<W, C>::flush()
{
    return writer_.flush();
}

} // end namespace acquire::storage:zarr

#endif //__cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_BLOSC_V0
