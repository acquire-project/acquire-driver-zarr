#include <iostream>
#include "acquire-zarr/acquire-zarr.hh"
#include "acquire-zarr-writer-impl.hh"
//#include "device/props/storage.h"


// forward declaration of some C API functions
//static int storage_properties_dimensions_init(struct StorageProperties* self, size_t size);

void AcquireZarrWriter::start()
{
    impl_->start();
}

void AcquireZarrWriter::append(uint8_t* image_data, size_t image_size)
{
    impl_->append(image_data, image_size);
}

void AcquireZarrWriter::set_use_v3(bool use_v3)
{
    impl_->zarr_version_ = use_v3 ? 3 : 2;
}

bool AcquireZarrWriter::get_use_v3() const
{
    return impl_->zarr_version_ == 3;
}

std::vector<uint32_t> AcquireZarrWriter::get_shape() const
{
    return {impl_->video_frame_.shape.dims.channels, 
        impl_->video_frame_.shape.dims.width, 
        impl_->video_frame_.shape.dims.height, 
        impl_->video_frame_.shape.dims.planes};
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


void AcquireZarrWriter::set_uri(const std::string& uri)
{
    //storage_properties_set_uri(&impl_->storage_properties_, uri.c_str(), uri.size());
    set_acquire_string(impl_->storage_properties_.uri, uri);
}

std::string AcquireZarrWriter::get_uri() const
{
    return get_from_acquire_string(impl_->storage_properties_.uri);
}

void AcquireZarrWriter::setExternalMetadata(const std::string& metadata)
{
    storage_properties_set_external_metadata(&impl_->storage_properties_, metadata.c_str(), metadata.size());
}

std::string AcquireZarrWriter::get_metadata() const
{
    return get_from_acquire_string(impl_->storage_properties_.external_metadata_json);
}

void AcquireZarrWriter::set_pixel_scale_x(double x)
{
    impl_->storage_properties_.pixel_scale_um.x = x;
}

double AcquireZarrWriter::get_pixel_scale_x() const
{
    return impl_->storage_properties_.pixel_scale_um.x;
}

double AcquireZarrWriter::get_pixel_scale_y() const
{
    return impl_->storage_properties_.pixel_scale_um.y;
}

void AcquireZarrWriter::set_pixel_scale_y(double y)
{
    impl_->storage_properties_.pixel_scale_um.y = y;
}

void AcquireZarrWriter::set_first_frame_id(uint32_t id)
{
    impl_->storage_properties_.first_frame_id = id;
}

uint32_t AcquireZarrWriter::get_first_frame_id() const
{
    return impl_->storage_properties_.first_frame_id;
}


void AcquireZarrWriter::set_dimensions(const std::vector<std::string>& dimensions)
{
    if (impl_->storage_properties_.acquisition_dimensions.size == 0)
    {
        //storage_properties_dimensions_init(&impl_->storage_properties_, dimensions.size());
        impl_->storage_properties_.acquisition_dimensions.size = dimensions.size();
        //impl_->storage_properties_.acquisition_dimensions.data = (struct StorageDimension*)malloc(sizeof(struct StorageDimension) * dimensions.size());
        impl_->storage_properties_.acquisition_dimensions.data = new StorageDimension[dimensions.size()];
    }

    for(auto i = 0; i < dimensions.size(); i++)
    {
        set_acquire_string(impl_->storage_properties_.acquisition_dimensions.data[i].name, dimensions[i]);

        switch(dimensions[i][0])
        {
            case 'x':
            case 'y':
            case 'z':
                impl_->storage_properties_.acquisition_dimensions.data[i].kind = DimensionType_Space;
                break;
            case 'c':
                impl_->storage_properties_.acquisition_dimensions.data[i].kind = DimensionType_Channel;
                break;
            case 't':
                impl_->storage_properties_.acquisition_dimensions.data[i].kind = DimensionType_Time;
                break;
            default:
                impl_->storage_properties_.acquisition_dimensions.data[i].kind = DimensionType_Other;

        }
    }

}

std::vector<std::string> AcquireZarrWriter::get_dimensions() const
{
    std::vector<std::string> dimensions;
    for(auto i = 0; i < impl_->storage_properties_.acquisition_dimensions.size; i++)
    {
        dimensions.push_back(get_from_acquire_string(impl_->storage_properties_.acquisition_dimensions.data[i].name));
    }
    return dimensions;
}

void AcquireZarrWriter::set_dimension_sizes(const std::vector<uint32_t>& dimensions)
{
    for(auto i = 0; i < dimensions.size(); i++)
    {
        impl_->storage_properties_.acquisition_dimensions.data[i].array_size_px = dimensions[i];
    }
}

std::vector<uint32_t> AcquireZarrWriter::get_dimension_sizes() const
{
    std::vector<uint32_t> sizes;
    for(auto i = 0; i < impl_->storage_properties_.acquisition_dimensions.size; i++)
    {
        sizes.push_back(impl_->storage_properties_.acquisition_dimensions.data[i].array_size_px);
    }
    return sizes;
}

std::vector<uint32_t> AcquireZarrWriter::get_chunk_sizes() const
{
    std::vector<uint32_t> sizes;
    for(auto i = 0; i < impl_->storage_properties_.acquisition_dimensions.size; i++)
    {
        sizes.push_back(impl_->storage_properties_.acquisition_dimensions.data[i].chunk_size_px);
    }
    return sizes;
}

void AcquireZarrWriter::set_chunk_sizes(const std::vector<uint32_t>& chunk_sizes)
{
    for(auto i = 0; i < chunk_sizes.size(); i++)
    {
        impl_->storage_properties_.acquisition_dimensions.data[i].chunk_size_px = chunk_sizes[i];
    }
}

std::vector<uint32_t> AcquireZarrWriter::get_shard_sizes() const
{
    std::vector<uint32_t> sizes;
    for(auto i = 0; i < impl_->storage_properties_.acquisition_dimensions.size; i++)
    {
        sizes.push_back(impl_->storage_properties_.acquisition_dimensions.data[i].shard_size_chunks);
    }
    return sizes;
}

void AcquireZarrWriter::set_shard_sizes(const std::vector<uint32_t>& shard_sizes)
{
    for(auto i = 0; i < shard_sizes.size(); i++)
    {
        impl_->storage_properties_.acquisition_dimensions.data[i].shard_size_chunks = shard_sizes[i];
    }
}

bool AcquireZarrWriter::get_enable_multiscale() const
{

    return impl_->storage_properties_.enable_multiscale != 0;
}

void AcquireZarrWriter::set_enable_multiscale(bool multiscale)
{
    impl_->storage_properties_.enable_multiscale = multiscale ? 1 : 0;
}

AcquireZarrCompressionCodec AcquireZarrWriter::get_compression_codec() const

{

    auto ret {AcquireZarrCompressionCodec::COMPRESSION_NONE};

    if(impl_->blosc_params_.codec_id == asz::compression_codec_as_string< asz::BloscCodecId::Lz4 >())
        ret = AcquireZarrCompressionCodec::COMPRESSION_BLOSC_LZ4;
    else if(impl_->blosc_params_.codec_id == asz::compression_codec_as_string< asz::BloscCodecId::Zstd >())
        ret = AcquireZarrCompressionCodec::COMPRESSION_BLOSC_ZSTD;


    return ret;
}

void AcquireZarrWriter::set_compression_codec(AcquireZarrCompressionCodec compression)
{
    switch (compression)
    {
        case AcquireZarrCompressionCodec::COMPRESSION_BLOSC_LZ4:
            impl_->blosc_params_.codec_id = asz::compression_codec_as_string< asz::BloscCodecId::Lz4 >();
            break;
        case AcquireZarrCompressionCodec::COMPRESSION_BLOSC_ZSTD:
            impl_->blosc_params_.codec_id = asz::compression_codec_as_string< asz::BloscCodecId::Zstd >();
            break;
        default:
            impl_->blosc_params_.codec_id = ""; // blank means no compression
    }
}

int AcquireZarrWriter::get_compression_level() const
{
    return impl_->blosc_params_.clevel;
}

void AcquireZarrWriter::set_compression_level(int level)
{
    impl_->blosc_params_.clevel = level;
}

int AcquireZarrWriter::get_compression_shuffle() const
{
    return impl_->blosc_params_.shuffle;
}


void AcquireZarrWriter::set_compression_shuffle(int shuffle)
{
    impl_->blosc_params_.shuffle = shuffle;
}


void AcquireZarrWriter::create_impl()
{
    impl_ = std::make_shared<Impl>();
}