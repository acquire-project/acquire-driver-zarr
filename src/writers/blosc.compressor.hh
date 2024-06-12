#ifndef H_ACQUIRE_ZARR_BLOSC_COMPRESSOR_V0
#define H_ACQUIRE_ZARR_BLOSC_COMPRESSOR_V0

#include "blosc.h"
#include "json.hpp"

namespace acquire::sink::zarr {
enum class BloscCodecId : uint8_t
{
    Lz4 = BLOSC_LZ4,
    Zstd = BLOSC_ZSTD
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

struct BloscCompressionParams
{
    static constexpr char id[] = "blosc";
    std::string codec_id;
    int clevel;
    int shuffle;

    BloscCompressionParams();
    BloscCompressionParams(const std::string& codec_id,
                           int clevel,
                           int shuffle);
};

void
to_json(nlohmann::json&, const BloscCompressionParams&);

void
from_json(const nlohmann::json&, BloscCompressionParams&);
}

#endif // H_ACQUIRE_ZARR_BLOSC_COMPRESSOR_V0
