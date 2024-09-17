#pragma once

#include "acquire.zarr.h"

#include <blosc.h>

#include <string>
#include <string_view>

namespace zarr {
const char*
blosc_codec_to_string(ZarrCompressionCodec codec);

struct BloscCompressionParams
{
    std::string codec_id;
    uint8_t clevel;
    uint8_t shuffle;

    BloscCompressionParams();
    BloscCompressionParams(std::string_view codec_id,
                           uint8_t clevel,
                           uint8_t shuffle);
};
} // namespace zarr
