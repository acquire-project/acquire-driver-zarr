#include "blosc.compression.params.hh"

const char*
zarr::blosc_codec_to_string(ZarrCompressionCodec codec)
{
    switch (codec) {
        case ZarrCompressionCodec_BloscZstd:
            return "zstd";
        case ZarrCompressionCodec_BloscLZ4:
            return "lz4";
        default:
            return "unrecognized codec";
    }
}

zarr::BloscCompressionParams::BloscCompressionParams(std::string_view codec_id,
                                                     uint8_t clevel,
                                                     uint8_t shuffle)
  : codec_id{ codec_id }
  , clevel{ clevel }
  , shuffle{ shuffle }
{
}
