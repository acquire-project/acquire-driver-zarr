#ifndef H_ZARR_STREAM
#define H_ZARR_STREAM

#include <cstddef> // size_t

#include "stream.settings.hh"

struct ZarrStream_s
{
    struct ZarrStreamSettings_s* settings;
    size_t version;
};

#endif // H_ZARR_STREAM
