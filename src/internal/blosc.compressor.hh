#pragma once

#include "zarr.h"

#include <blosc.h>

#include <string>
#include <string_view>

namespace zarr {
template<ZarrCompressionCodec CodecId>
constexpr const char*
compression_codec_as_string();

template<>
constexpr const char*
compression_codec_as_string<ZarrCompressionCodec_BloscZstd>()
{
    return "zstd";
}

template<>
constexpr const char*
compression_codec_as_string<ZarrCompressionCodec_BloscLZ4>()
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
    BloscCompressionParams(std::string_view codec_id, int clevel, int shuffle);
};
} // namespace zarr
