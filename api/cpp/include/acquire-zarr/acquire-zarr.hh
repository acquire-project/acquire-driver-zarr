#ifndef ACQUIRE_ZARR_HH
#define ACQUIRE_ZARR_HH

#include <memory>
#include <vector>
/*
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


*/

class AcquireZarrWriter 
{
  public:
    AcquireZarrWriter() { create_impl();}
    ~AcquireZarrWriter() = default;

    void open();
    void append(uint8_t* image_data, size_t image_size);


    // Getters and setters for configuration
    
    /**
     * Get the shape of the image data.
     * 
     * @returns array corresponding to the shape of the image data (channels, width, height, planes)
     * 
     */
    std::vector<uint32_t> get_shape() const;

    /**
     * Set the shape of the image data.
     * 
     * @param shape array corresponding to the shape of the image data (channels, width, height, planes)
     */
    void set_shape(const std::vector<uint32_t>& shape);

  private:
    
    /**
     * @brief Is Zarr file is open?  
     * A parameter validation check is performed when opening, so configuration should fail after opening. 
     */
    bool is_open_ = false;

    /**
     * Forward declaration of internal implmentation class
     */
    class Impl;

    /**
     * @brief Create the internal implementation of the AcquireZarrWriter class
     * 
     */
    virtual void create_impl();

    /**
     * @brief pointer to internal implementation of the AcquireZarrWriter class
     *  
     * @note despite lack of shared ownership, couldn't use unique_ptr because of 
     * incomplete type (forward declaration of Impl)
     * 
     */
    // 
    // std::unique_ptr<Impl> impl_;  (DOESN'T BUILD)
    std::shared_ptr<Impl> impl_;
    
}; 

#endif //ACQUIRE_ZARR_HH
