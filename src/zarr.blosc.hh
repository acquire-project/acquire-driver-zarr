#ifndef H_ACQUIRE_STORAGE_ZARR_BLOSC_V0
#define H_ACQUIRE_STORAGE_ZARR_BLOSC_V0

#ifdef __cplusplus

#include "zarr.encoder.hh"

#include "blosc.h"

namespace acquire::sink::zarr {

struct BloscCompressor
{
    static constexpr char id_[] = "blosc";
    std::string codec_id_;
    int clevel_;
    int shuffle_;

    BloscCompressor();
    BloscCompressor(const std::string& codec_id, int clevel, int shuffle);
};

void
to_json(nlohmann::json&, const BloscCompressor&);

void
from_json(const nlohmann::json&, BloscCompressor&);

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

struct BloscEncoder final : public BaseEncoder
{
  public:
    explicit BloscEncoder(const BloscCompressor& compressor);
    ~BloscEncoder() noexcept override;

  private:
    size_t flush_impl() override;

    BloscCompressor compressor_;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_BLOSC_V0