#ifndef H_ACQUIRE_STORAGE_BLOSC_ENCODER_V0
#define H_ACQUIRE_STORAGE_BLOSC_ENCODER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "encoder.hh"

#include "blosc.h"

#include <thread>

namespace {
enum class CodecId : uint8_t
{
    Lz4 = 1,
    Zstd = 5
};

template<CodecId CodecId>
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
}

namespace acquire::sink::zarr {
template<CodecId CodecId, int CLevel, int Shuffle>
struct BloscEncoder final
{
  public:
    BloscEncoder() = default;
    explicit BloscEncoder(SampleType sample_type);
    ~BloscEncoder() = default;

    /// Encoder
    size_t encode(uint8_t* bytes_out,
                  size_t nbytes_out,
                  uint8_t* bytes_in,
                  size_t nbytes_in) const;

  private:
    SampleType sample_type_;
};

template<CodecId CodecId, int CLevel, int Shuffle>
BloscEncoder<CodecId, CLevel, Shuffle>::BloscEncoder(SampleType sample_type)
  : sample_type_{ sample_type }
{
}

template<CodecId CodecId, int CLevel, int Shuffle>
size_t
BloscEncoder<CodecId, CLevel, Shuffle>::encode(uint8_t* bytes_out,
                                               size_t nbytes_out,
                                               uint8_t* bytes_in,
                                               size_t nbytes_in) const
{
    CHECK(bytes_out);
    CHECK(bytes_in);

    CHECK(nbytes_in > 0);
    auto max_bytes_out = nbytes_in + BLOSC_MAX_OVERHEAD;
    CHECK(nbytes_out >= max_bytes_out);

    return (size_t)blosc_compress_ctx(CLevel,
                                      Shuffle,
                                      1, // FIXME (aliddell) bytes_per_pixel_,
                                      nbytes_in,
                                      bytes_in,
                                      bytes_out,
                                      nbytes_in,
                                      max_bytes_out,
                                      compression_codec_as_string<CodecId>(),
                                      0 /* blocksize - 0:automatic */,
                                      (int)std::thread::hardware_concurrency());
}
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_BLOSC_ENCODER_V0
