#ifndef ACQUIRE_ZARR_H
#define ACQUIRE_ZARR_H

#include <stdint.h>

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

// Forward declaration of AcquireZarrSinkWrapper
struct AcquireZarrSinkWrapper;

// Compression options for Zarr
enum AcquireZarrCompression
{
    AcquireZarrCompression_NONE = 0,
    AcquireZarrCompression_BLOSC
};

// Zarr Version
enum AcquireZarrVersion
{
    AcquireZarrVersion_2 = 2,
    AcquireZarrVersion_3 = 3
};

// Configuration for Zarr sink
struct AcquireZarrSinkConfig
{
    char filename[512];
    enum AcquireZarrCompression compression;
    int multiscale; // 0 or 1, because I don't have bool in C
};


// 

// Open a Zarr sink with the given configuration
EXTERNC struct AcquireZarrSinkWrapper* zarr_sink_open(const struct AcquireZarrSinkConfig* config);

// Close the Zarr sink
EXTERNC void zarr_sink_close();

EXTERNC int zarr_sink_append(struct AcquireZarrSinkWrapper*, uint8_t* image_data, uint8_t dimensions, uint16_t shape);


#endif // ACQUIRE_ZARR_H