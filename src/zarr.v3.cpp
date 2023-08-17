#include "zarr.hh"

namespace zarr = acquire::sink::zarr;

extern "C" struct Storage*
zarr_v3_init()
{
    try {
        return new zarr::ZarrV3();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}