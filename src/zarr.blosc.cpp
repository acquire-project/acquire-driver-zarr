#include "zarr.blosc.hh"
#include "zarr.hh"

#include "logger.h"

#include <stdexcept>
#include <thread>

namespace zarr = acquire::sink::zarr;
using json = nlohmann::json;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_init()
{
    try {
        zarr::CompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::Zarr(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}

template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_v3_init()
{
    try {
        zarr::CompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::ZarrV3(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

//
// zarr namespace implementations
//

void
zarr::to_json(json& j, const zarr::CompressionParams& bc)
{
    j = json{ { "id", std::string(bc.id_) },
              { "cname", bc.codec_id_ },
              { "clevel", bc.clevel_ },
              { "shuffle", bc.shuffle_ } };
}

void
zarr::from_json(const json& j, zarr::CompressionParams& bc)
{
    j.at("cname").get_to(bc.codec_id_);
    j.at("clevel").get_to(bc.clevel_);
    j.at("shuffle").get_to(bc.shuffle_);
}

zarr::BloscEncoder::BloscEncoder(const CompressionParams& compressor)
  : compressor_{ compressor }
{
}

zarr::BloscEncoder::~BloscEncoder() noexcept
{
    try {
        flush();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

size_t
zarr::BloscEncoder::flush_impl()
{
    auto* buf_c = new uint8_t[cursor_ + BLOSC_MAX_OVERHEAD];
    CHECK(buf_c);

    const auto nbytes_out =
      (size_t)blosc_compress_ctx(compressor_.clevel_,
                                 compressor_.shuffle_,
                                 bytes_per_pixel_,
                                 cursor_,
                                 buf_.data(),
                                 buf_c,
                                 cursor_ + BLOSC_MAX_OVERHEAD,
                                 compressor_.codec_id_.c_str(),
                                 0 /* blocksize - 0:automatic */,
                                 (int)std::thread::hardware_concurrency());

    CHECK(file_write(file_, 0, buf_c, buf_c + nbytes_out));

    delete[] buf_c;
    return nbytes_out;
}

extern "C" {
    struct Storage*
    compressed_zarr_zstd_init()
    {
        return compressed_zarr_init<zarr::BloscCodecId::Zstd>();
    }

    struct Storage*
    compressed_zarr_lz4_init()
    {
        return compressed_zarr_init<zarr::BloscCodecId::Lz4>();
    }

    struct Storage*
    compressed_zarr_v3_zstd_init()
    {
        return compressed_zarr_v3_init<zarr::BloscCodecId::Zstd>();
    }

    struct Storage*
    compressed_zarr_v3_lz4_init()
    {
        return compressed_zarr_v3_init<zarr::BloscCodecId::Lz4>();
    }
}


