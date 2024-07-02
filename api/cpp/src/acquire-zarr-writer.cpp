#include <iostream>
#include "acquire-zarr-writer-impl.hh"



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