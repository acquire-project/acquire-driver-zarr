#include "acquire-zarr/acquire-zarr.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ZarrSink
{
    int a;
    int b;
} ;

struct ZarrSink* zarr_sink_open()
{
    struct ZarSink* myptr = 0;
    return myptr;
    
}

void zarr_sink_close()
{

}

int zarr_sink_append(struct ZarrSink*, uint8_t* image_data, uint8_t dimensions, uint16_t shape, uint8_t chunk_dimensions, uint16_t chunk_shape)
{
}

#ifdef __cplusplus
}
#endif

