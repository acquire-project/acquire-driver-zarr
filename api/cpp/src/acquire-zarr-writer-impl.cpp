#include "acquire-zarr-writer-impl.hh"
#include <iostream>

//using namespace acquire::sink::zarr;


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