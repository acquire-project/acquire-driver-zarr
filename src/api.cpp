#include "acquire-zarr/acquire-zarr.h"
#include "zarr.v2.hh"



EXTERNC struct AcquireZarrSinkWrapper* zarr_sink_open(const struct AcquireZarrSinkConfig* config)
{
    struct AcquireZarrSinkWrapper* myptr = new AcquireZarrSinkWrapper(config);
    
    //myptr->open();
    return myptr;
    
}

EXTERNC void zarr_sink_close()
{

}

EXTERNC int zarr_sink_append(struct AcquireZarrSinkWrapper*, uint8_t* image_data, uint8_t dimensions, uint16_t shape)
{
}



// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
using namespace acquire::sink::zarr;

struct AcquireZarrSinkWrapper 
{
  public:
    AcquireZarrSinkWrapper(const struct AcquireZarrSinkConfig* config);
    ~AcquireZarrSinkWrapper();

    int open();
  protected:
    AcquireZarrSinkConfig config_;
    std::shared_ptr<struct Zarr> zarr_sink_ = nullptr;
};

AcquireZarrSinkWrapper::AcquireZarrSinkWrapper(const struct AcquireZarrSinkConfig* config)
{
    config_ = *config;

    if (config_.compression != AcquireZarrCompression_NONE)
    {
        // Set the compression parameters
        BloscCompressionParams blosc_params;
        blosc_params.codec_id = (config_.compression == AcquireZarrCompression_BLOSC_LZ4) ? 
            compression_codec_as_string< BloscCodecId::Lz4 >() : 
            compression_codec_as_string< BloscCodecId::Zstd >();

        blosc_params.clevel = 1;
        blosc_params.shuffle = 1;
        zarr_sink_ = std::make_shared<Zarr>(blosc_params);

    }
    else
    {
        // No compression
        zarr_sink_ = std::make_shared<Zarr>();
    }
}


AcquireZarrSinkWrapper::~AcquireZarrSinkWrapper()
{
}

int AcquireZarrSinkWrapper::open()
{

    zarr_ = std::make_shared<Zarr>(config_.filename);
    return 0;
}