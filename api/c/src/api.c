#include "acquire-zarr/acquire-zarr.hh"

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
