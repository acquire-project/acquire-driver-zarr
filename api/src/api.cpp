#include <iostream>
#include "acquire-zarr/acquire-zarr.h"
#include "zarr.v2.hh"
#include "zarr.v3.hh"


// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
using namespace acquire::sink::zarr;

struct AcquireZarrSinkWrapper 
{
  public:
    AcquireZarrSinkWrapper() = default;
    ~AcquireZarrSinkWrapper() = default;

    void configure(const struct AcquireZarrSinkConfig* config);
    void open();
    void append(uint8_t* image_data, size_t image_size);
  protected:
    AcquireZarrSinkConfig config_;

    // use shared_ptr for polymorphism of zarr version.
    std::shared_ptr<struct Zarr> zarr_sink_ = nullptr;

    struct VideoFrame video_frame_;

}; 


void AcquireZarrSinkWrapper::configure(const struct AcquireZarrSinkConfig* config)
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

    // todo: get these from config data structure
    video_frame_.shape.dims.channels = config_.shape.channels;
    video_frame_.shape.dims.width = config_.shape.width;
    video_frame_.shape.dims.height = config_.shape.height;
    video_frame_.shape.dims.planes = config_.shape.planes;
    video_frame_.shape.strides.channels = 1;
    video_frame_.shape.strides.width = video_frame_.shape.dims.channels ;
    video_frame_.shape.strides.height = video_frame_.shape.dims.width * video_frame_.shape.strides.width;
    video_frame_.shape.strides.planes = video_frame_.shape.dims.height * video_frame_.shape.strides.height;;
    video_frame_.shape.type = SampleType_u8;  
    video_frame_.bytes_of_frame = sizeof(video_frame_) + video_frame_.shape.dims.planes * video_frame_.shape.strides.planes;

}

void AcquireZarrSinkWrapper::open()
{
    zarr_sink_->start();
}

void AcquireZarrSinkWrapper::append(uint8_t* image_data, size_t image_size)
{

    // todo: check image size against expected size
    memcpy(video_frame_.data, image_data, image_size);
    const VideoFrame* const_frame = &video_frame_;
    zarr_sink_->append(const_frame, image_size);
}

EXTERNC struct AcquireZarrSinkWrapper* zarr_sink_open(const struct AcquireZarrSinkConfig* config)
{
    try
    {
        struct AcquireZarrSinkWrapper* newptr = new struct AcquireZarrSinkWrapper();

        newptr->configure(config);
        newptr->open();
        return newptr;

    }
    catch (...)
    {
        LOG("Error opening Zarr sink");
        return NULL;
    }

    
}

EXTERNC void zarr_sink_close(struct AcquireZarrSinkWrapper* zarr_sink)
{
    try
    {
        
        if (zarr_sink != NULL)
        {
            delete zarr_sink;
            zarr_sink = nullptr;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

EXTERNC int zarr_sink_append(struct AcquireZarrSinkWrapper* zarr_sink, uint8_t* image_data, size_t image_size)
{
    try
    {
        if(image_size != zarr_sink->video_frame_.shape.dims.width * zarr_sink->video_frame_.shape.dims.height)
        {
            throw std::runtime_error("Image size does not match expected size");
        };

        zarr_sink->append(image_data, image_size);
        return 0;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return -1;
    }
    
}
