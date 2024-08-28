#include "blosc.compressor.hh"

zarr::BloscCompressionParams::BloscCompressionParams()
  : clevel{ 1 }
  , shuffle{ 1 }
{
}

zarr::BloscCompressionParams::BloscCompressionParams(std::string_view codec_id,
                                                     int clevel,
                                                     int shuffle)
  : codec_id{ codec_id }
  , clevel{ clevel }
  , shuffle{ shuffle }
{
}
