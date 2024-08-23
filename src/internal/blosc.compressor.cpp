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

//void
//zarr::to_json(json& j, const zarr::BloscCompressionParams& bcp)
//{
//    j = json{ { "id", std::string(BloscCompressionParams::id) },
//              { "cname", bcp.codec_id },
//              { "clevel", bcp.clevel },
//              { "shuffle", bcp.shuffle } };
//}
//
//void
//zarr::from_json(const json& j, zarr::BloscCompressionParams& bcp)
//{
//    j.at("cname").get_to(bcp.codec_id);
//    j.at("clevel").get_to(bcp.clevel);
//    j.at("shuffle").get_to(bcp.shuffle);
//}