#include <iostream>
#include "acquire-zarr/acquire-zarr.hh"
#include "zarr.v2.hh"
#include "zarr.v3.hh"


// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
using namespace acquire::sink::zarr;

class AcquireZarrWriter::Impl
{
  public:
    Impl() = default;
    ~Impl() = default;

    void open();
    void append(uint8_t* image_data, size_t image_size);

    friend AcquireZarrWriter;
  protected:

    // use shared_ptr for polymorphism of zarr version.
    std::unique_ptr<struct Zarr> zarr_sink_;

  // video frame needs to be the last member of the struct
    struct VideoFrame video_frame_;
}; 

/*
void AcquireZarrWriter::Impl::configure(const struct AcquireZarrSinkConfig* config)
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
*/ 
void AcquireZarrWriter::Impl::open()
{
    zarr_sink_->start();
}

void AcquireZarrWriter::Impl::append(uint8_t* image_data, size_t image_size)
{

    // deleteme:
    std::cout << "Appending image of size " << image_size << std::endl;
    for (int i = 0; i < image_size; i++)
    {
        std::cout << (int)image_data[i] << " ";
    }
    std::cout << std::endl;
    return;

    // todo: check image size against expected size
    memcpy(video_frame_.data, image_data, image_size);
    const VideoFrame* const_frame = &video_frame_;
    zarr_sink_->append(const_frame, image_size);
}

/*
void AcquireZarrWriter::configure(const struct AcquireZarrSinkConfig* config)
{
    impl_ = std::make_shared<Impl>();
    //impl_->configure(config);
}
*/


void AcquireZarrWriter::open()
{
    impl_->open();
}

void AcquireZarrWriter::append(uint8_t* image_data, size_t image_size)
{
    impl_->append(image_data, image_size);
}

std::vector<uint32_t> AcquireZarrWriter::get_shape() const
{
    return {impl_->video_frame_.shape.dims.channels, impl_->video_frame_.shape.dims.width, impl_->video_frame_.shape.dims.height, impl_->video_frame_.shape.dims.planes};
}

void AcquireZarrWriter::set_shape(const std::vector<uint32_t>& shape)
{
    impl_->video_frame_.shape.dims.channels = shape[0];
    impl_->video_frame_.shape.dims.width = shape[1];
    impl_->video_frame_.shape.dims.height = shape[2];
    impl_->video_frame_.shape.dims.planes = shape[3];
    impl_->video_frame_.shape.strides.channels = 1;
    impl_->video_frame_.shape.strides.width = impl_->video_frame_.shape.dims.channels ;
    impl_->video_frame_.shape.strides.height = impl_->video_frame_.shape.dims.width * impl_->video_frame_.shape.strides.width;
    impl_->video_frame_.shape.strides.planes = impl_->video_frame_.shape.dims.height * impl_->video_frame_.shape.strides.height;
    impl_->video_frame_.shape.type = SampleType_u8;  
    impl_->video_frame_.bytes_of_frame = sizeof(impl_->video_frame_) + impl_->video_frame_.shape.dims.planes * impl_->video_frame_.shape.strides.planes;
}

void AcquireZarrWriter::create_impl()
{
    impl_ = std::make_shared<Impl>();
}