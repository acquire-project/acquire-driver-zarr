#include "acquire-zarr/acquire-zarr.h"
#include "zarr.hh"



EXTERNC struct ZarrSink* zarr_sink_open(const struct ZarrSinkConfig* config)
{
    struct ZarrSink* myptr = 0;
    return myptr;
    
}

EXTERNC void zarr_sink_close()
{

}

EXTERNC int zarr_sink_append(struct ZarrSink*, uint8_t* image_data, uint8_t dimensions, uint16_t shape)
{
}



// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
struct ZarrSink 
{
    ZarrSink(const struct ZarrSinkConfig* config);
    ~ZarrSink();

    ZarrSinkConfig config_;
    std::shared_ptr<struct Zarr> zarr_ = nullptr;
};

ZarrSink::ZarrSink(const struct ZarrSinkConfig* config)
{
    if(std::memcpy(&config_, config, sizeof(ZarrSinkConfig)) != &config_)
    {
        throw std::runtime_error("Failed to copy ZarrSinkConfig");
    }
}