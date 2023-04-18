#include "zarr.raw.hh"
#include "zarr.codec.hh"
#include "zarr.blosc.hh"
#include "zarr.hh"

#include "platform.h"
#include "logger.h"

#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::CodecId C>
struct Storage*
compressed_zarr_init()
{
    try {
        return new zarr::Zarr<
          zarr::Buffer<zarr::BloscEncoder<zarr::RawFile, C>>>();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
}

extern "C" struct Storage*
compressed_zarr_zstd_init()
{
    return compressed_zarr_init<zarr::CodecId::Zstd>();
}

extern "C" struct Storage*
compressed_zarr_lz4_init()
{
    return compressed_zarr_init<zarr::CodecId::Lz4>();
}
