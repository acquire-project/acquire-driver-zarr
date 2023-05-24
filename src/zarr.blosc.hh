#ifndef H_ACQUIRE_STORAGE_ZARR_BLOSC_V0
#define H_ACQUIRE_STORAGE_ZARR_BLOSC_V0

#ifdef __cplusplus

#include "zarr.encoder.hh"

#include <thread>

#include "blosc.h"

namespace acquire::sink::zarr {

enum class BloscCodecId
{
    Lz4 = 0,
    Zstd,
};

template<BloscCodecId CodecId>
constexpr const char*
compression_codec_as_string();

template<>
constexpr const char*
compression_codec_as_string<BloscCodecId::Zstd>()
{
    return "zstd";
}

template<>
constexpr const char*
compression_codec_as_string<BloscCodecId::Lz4>()
{
    return "lz4";
}

template<BloscCodecId CodecId, int CLevel, int Shuffle>
struct BloscEncoder final : public BaseEncoder
{
  public:
    BloscEncoder();
    ~BloscEncoder() noexcept override;

    BloscCompressor* get_compressor() override;

  private:
    size_t flush_impl() override;
    void open_file_impl() override;

    BloscCompressor compressor_;
};

template<BloscCodecId CodecId, int CLevel, int Shuffle>
BloscEncoder<CodecId, CLevel, Shuffle>::BloscEncoder()
  : compressor_(compression_codec_as_string<CodecId>(), CLevel, Shuffle)
{
}

template<BloscCodecId CodecId, int CLevel, int Shuffle>
BloscEncoder<CodecId, CLevel, Shuffle>::~BloscEncoder() noexcept
{
    flush();
    close_file();
}

template<BloscCodecId CodecId, int CLevel, int Shuffle>
BloscCompressor*
BloscEncoder<CodecId, CLevel, Shuffle>::get_compressor()
{
    return &compressor_;
}

template<BloscCodecId CodecId, int CLevel, int Shuffle>
size_t
BloscEncoder<CodecId, CLevel, Shuffle>::flush_impl()
{
    auto* buf_c = new uint8_t[cursor_ + BLOSC_MAX_OVERHEAD];
    CHECK(buf_c);

    const auto nbytes_out =
      (size_t)blosc_compress_ctx(CLevel,
                                 Shuffle,
                                 bytes_per_pixel_,
                                 cursor_,
                                 buf_.data(),
                                 buf_c,
                                 cursor_ + BLOSC_MAX_OVERHEAD,
                                 compression_codec_as_string<CodecId>(),
                                 0 /* blocksize - 0:automatic */,
                                 (int)std::thread::hardware_concurrency());

    CHECK(file_write(file_handle_, 0, buf_c, buf_c + nbytes_out));

    delete[] buf_c;
    return nbytes_out;
}

template<BloscCodecId CodecId, int CLevel, int Shuffle>
void
BloscEncoder<CodecId, CLevel, Shuffle>::open_file_impl()
{
}
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_BLOSC_V0