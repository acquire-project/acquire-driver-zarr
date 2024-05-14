#ifndef ACQUIRE_ZARR_H
#define ACQUIRE_ZARR_H

#include <stdint.h>

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

// Forward declaration of ZarrSink
struct ZarrSink;

// Compression options for Zarr
enum AcquireZarrCompression
{
    AcquireZarrCompression_NONE = 0,
    AcquireZarrCompression_ZSTD,
    AcquireZarrCompression_BLOSC
} ;

// Configuration for Zarr sink
struct ZarrSinkConfig
{
    char filename[512];
    enum AcquireZarrCompression compression;
    int multiscale; // 0 or 1, because I don't have bool in C
} ;

// Open a Zarr sink with the given configuration
EXTERNC struct ZarrSink* zarr_sink_open(const struct ZarrSinkConfig* config);

// Close the Zarr sink
EXTERNC void zarr_sink_close();

EXTERNC int zarr_sink_append(struct ZarrSink*, uint8_t* image_data, uint8_t dimensions, uint16_t shape);


#endif // ACQUIRE_ZARR_H