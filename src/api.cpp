#include "acquire-zarr/acquire-zarr.h"
#include "zarr.v2.hh"
#include "zarr.v3.hh"



// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
using namespace acquire::sink::zarr;

struct AcquireZarrSinkWrapper 
{
  public:
    AcquireZarrSinkWrapper(const struct AcquireZarrSinkConfig* config);
    ~AcquireZarrSinkWrapper() = default;

    int open();
  protected:
    AcquireZarrSinkConfig config_;

    // use shared_ptr for polymorphism of zarr version.
    std::shared_ptr<struct Zarr> zarr_sink_ = nullptr;
};


AcquireZarrSinkWrapper::AcquireZarrSinkWrapper(const struct AcquireZarrSinkConfig* config)
{
    config_ = *config;

    if (config_.compression == AcquireZarrSinkConfig::AcquireZarrCompression::AcquireZarrCompression_NONE)
    {

        // No compression
        if (config_.zarr_version==AcquireZarrSinkConfig::AcquireZarrVersion::AcquireZarrVersion_2)
            zarr_sink_ = std::make_shared<struct ZarrV2>();
        else
            zarr_sink_ = std::make_shared<struct ZarrV3>();
    }
    else
    {
        // Set the compression parameters
        struct BloscCompressionParams blosc_params;
        blosc_params.codec_id = (config_.compression == AcquireZarrSinkConfig::AcquireZarrCompression::AcquireZarrCompression_BLOSC_LZ4) ? 
            compression_codec_as_string< BloscCodecId::Lz4 >() : 
            compression_codec_as_string< BloscCodecId::Zstd >();

        // todo: parameterize clvel and shuffle?
        blosc_params.clevel = 1;
        blosc_params.shuffle = 1;
        

        if (config_.zarr_version==AcquireZarrSinkConfig::AcquireZarrVersion::AcquireZarrVersion_2)
            zarr_sink_ = std::make_shared<struct ZarrV2>(std::move(blosc_params));
        else
            zarr_sink_ = std::make_shared<struct ZarrV3>(std::move(blosc_params));
        }

}


int AcquireZarrSinkWrapper::open()
{
    return 0;
}


EXTERNC struct AcquireZarrSinkWrapper* zarr_sink_open(const struct AcquireZarrSinkConfig* config)
{
    auto myptr = new AcquireZarrSinkWrapper(config);
    
    //myptr->open();
    return myptr;
    
}

EXTERNC void zarr_sink_close()
{

}

EXTERNC int zarr_sink_append(struct AcquireZarrSinkWrapper*, uint8_t* image_data, uint8_t dimensions, uint16_t shape)
{
    return -1;
}
