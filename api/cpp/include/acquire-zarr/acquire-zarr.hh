#ifndef ACQUIRE_ZARR_HH
#define ACQUIRE_ZARR_HH

#include <memory>

// Configuration for Zarr sink
struct AcquireZarrSinkConfig
{
    char filename[512];

    struct shape_s
    {
        uint32_t channels, width, height, planes;
    } shape;

    // 8, 12, 14, 16, 32  (non power of 2 values are rounded up to the next power of 2)
    uint8_t sample_bits;  

    enum SampleType
    {
        UNKNOWN,
        UNSIGNED_INT,
        SIGNED_INT,
        FLOAT
    } word_type;    

    // Zarr Version
    enum AcquireZarrVersion
    {
        AcquireZarrVersion_2,
        AcquireZarrVersion_3
    } zarr_version;

    // Compression options for Zarr
    enum AcquireZarrCompression
    {
        AcquireZarrCompression_NONE,
        AcquireZarrCompression_BLOSC_LZ4,
        AcquireZarrCompression_BLOSC_ZSTD,
    } compression;

    uint8_t multiscale; // 0 or 1, because I don't have bool in C
};

class AcquireZarrWriter 
{
  public:
    AcquireZarrWriter() = default;
    ~AcquireZarrWriter() = default;

    void configure(const struct AcquireZarrSinkConfig* config);
    void open();
    void append(uint8_t* image_data, size_t image_size);

  private:
    
    class Impl;

    // couldn't use unique_ptr because of incomplete type
    // std::unique_ptr<Impl> impl_;
    std::shared_ptr<Impl> impl_;

}; 

#endif //ACQUIRE_ZARR_HH
