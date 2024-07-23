#include "acquire-zarr-writer-impl.hh"
#include <iostream>
#include "zarr.v2.hh"
#include "zarr.v3.hh"

void set_acquire_string(String& dst, const std::string& src)
{
    dst.nbytes = src.size() + 1; // +1 for null terminator
    dst.str = new char[dst.nbytes];
    strncpy(dst.str, src.c_str(), src.size());
    dst.str[src.size()] = '\0';
    dst.is_ref = 0;
}

std::string get_from_acquire_string(const String& src)
{
    return std::string(src.str);
}


AcquireZarrWriter::Impl::Impl()
{
    memset(&storage_properties_, 0, sizeof(struct StorageProperties));
    memset(&shape_, 0, sizeof(struct ImageShape));

    // Default to v2
    zarr_version_ = 2;
}

void AcquireZarrWriter::Impl::start()
{

    create_zarr_sink();

    zarr_sink_->set(&storage_properties_);

    assert(storage_properties_.acquisition_dimensions.size > 0);
    shape_.type = SampleType_u8;

    shape_.dims.channels = storage_properties_.acquisition_dimensions.size - 1 ;  // one less than the size, since the last dimension is append
    
    shape_.dims.width =  storage_properties_.acquisition_dimensions.data[0].array_size_px;
    shape_.dims.height = storage_properties_.acquisition_dimensions.data[1].array_size_px;
    shape_.dims.planes = 
        storage_properties_.acquisition_dimensions.data[2].array_size_px > 1 ? 
        storage_properties_.acquisition_dimensions.data[2].array_size_px : 
        1;

     shape_.dims.channels = 
        storage_properties_.acquisition_dimensions.data[3].array_size_px > 1 ? 
        storage_properties_.acquisition_dimensions.data[3].array_size_px : 
        1;   

    zarr_sink_->reserve_image_shape(&shape_);
    zarr_sink_->start();
}


void AcquireZarrWriter::Impl::append(const uint8_t* image_data, size_t image_size)
{

    // todo: check image size against expected size?
    auto written = zarr_sink_->append_frame(image_data, image_size, shape_);
}

void AcquireZarrWriter::Impl::create_zarr_sink()
{

    if ((strcmp(blosc_params_.codec_id.c_str() , asz::compression_codec_as_string< asz::BloscCodecId::Lz4 >()) == 0) || 
        (strcmp(blosc_params_.codec_id.c_str(), asz::compression_codec_as_string< asz::BloscCodecId::Zstd >()) == 0) )
    {        

        if (zarr_version_==2)
            zarr_sink_ = std::make_shared<struct asz::ZarrV2>(std::move(blosc_params_));
        else
            zarr_sink_ = std::make_shared<struct asz::ZarrV3>(std::move(blosc_params_));
    }
    else
    {
        // No compression
        if (zarr_version_==2)
            zarr_sink_ = std::make_shared<struct asz::ZarrV2>();
        else
            zarr_sink_ = std::make_shared<struct asz::ZarrV3>();
    }
}
