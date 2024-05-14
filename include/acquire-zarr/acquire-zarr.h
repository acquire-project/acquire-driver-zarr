#ifndef ACQUIRE_ZARR_H
#define ACQUIRE_ZARR_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ZarrSink;

struct ZarrSink* zarr_sink_open();

void zarr_sink_close();

int zarr_sink_append(struct ZarrSink*, uint8_t* image_data, uint8_t dimensions, uint16_t shape, uint8_t chunk_dimensions, uint16_t chunk_shape);


#ifdef __cplusplus
}
#endif

#endif // ACQUIRE_ZARR_H