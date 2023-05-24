#include "zarr.blosc.hh"
#include "zarr.hh"

#include "logger.h"

#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_init()
{
    try {
        return new zarr::Zarr<zarr::BloscEncoder<CodecId, 1, 1>>();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

extern "C" struct Storage*
compressed_zarr_zstd_init()
{
    return compressed_zarr_init<zarr::BloscCodecId::Zstd>();
}

extern "C" struct Storage*
compressed_zarr_lz4_init()
{
    return compressed_zarr_init<zarr::BloscCodecId::Lz4>();
}
