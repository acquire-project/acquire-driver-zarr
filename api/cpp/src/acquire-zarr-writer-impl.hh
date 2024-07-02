#include "acquire-zarr/acquire-zarr.hh"
#include "zarr.v2.hh"
//#include "zarr.v3.hh"

using namespace acquire::sink::zarr;

// C++ implementation of the ZarrSink, which is just a wrapper around the Zarr storage class heirarchy.
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
