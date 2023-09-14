#include "czar.hh"

#include "writers/chunk.writer.hh"

namespace zarr = acquire::sink::zarr;

extern "C" struct Storage*
czar_init()
{
    try {
        return new zarr::Czar<2, zarr::ChonkWriter>();
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}

#ifndef NO_UNIT_TESTS

#endif
