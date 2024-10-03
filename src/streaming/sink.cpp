#include "sink.hh"

bool
zarr::finalize_sink(std::unique_ptr<zarr::Sink>&& sink)
{
    if (!sink->flush_()) {
        return false;
    }

    sink.reset();
    return true;
}