#include "blosc.compression.params.hh"

zarr::BloscCompressionParams::BloscCompressionParams()
  : clevel{ 1 }
  , shuffle{ 1 }
{
}

zarr::BloscCompressionParams::BloscCompressionParams(std::string_view codec_id,
                                                     uint8_t clevel,
                                                     uint8_t shuffle)
  : codec_id{ codec_id }
  , clevel{ clevel }
  , shuffle{ shuffle }
{
}
