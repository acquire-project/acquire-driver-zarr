#ifndef ACQUIRE_ZARR_WRITER_IMPL_HH
#define ACQUIRE_ZARR_WRITER_IMPL_HH


#include "acquire-zarr/acquire-zarr.hh"
#include "zarr.hh"

namespace asz = acquire::sink::zarr;

void set_acquire_string(String& dst, const std::string& src);
std::string get_from_acquire_string(const String& src);

// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
class AcquireZarrWriter::Impl
{
  public:
    Impl();
    ~Impl() = default;

    void open();
    void append(uint8_t* image_data, size_t image_size);

    friend AcquireZarrWriter;
  protected:

    /// @brief Create the Zarr sink object
    virtual void create_zarr_sink();

    /// @brief Zarr version
    uint8_t zarr_version_;

    // use shared_ptr for polymorphism of zarr version.
    std::shared_ptr<struct asz::Zarr> zarr_sink_;

    /// @brief Data structure to hold the storage properties for the Zarr sink
    struct StorageProperties storage_properties_;

    struct asz::BloscCompressionParams blosc_params_;

    // video frame needs to be the last member of the struct
    struct VideoFrame video_frame_;


}; 

#endif // ACQUIRE_ZARR_WRITER_IMPL_HH
