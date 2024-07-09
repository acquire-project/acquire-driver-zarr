#ifndef ACQUIRE_ZARR_HH
#define ACQUIRE_ZARR_HH

#include <memory>
#include <vector>


enum class AcquireZarrCompressionCodec
{
    COMPRESSION_NONE,
    COMPRESSION_BLOSC_LZ4,
    COMPRESSION_BLOSC_ZSTD,
} ;



class AcquireZarrWriter 
{
  public:
    AcquireZarrWriter() { create_impl();}
    ~AcquireZarrWriter() = default;

    void start();
    void append(uint8_t* image_data, size_t image_size);


    // Getters and setters for configuration
    

    /**
     * Set the Zarr version to use.
     * 
     * @param use_v3 true to use Zarr version 3, false to use version 2
     */
    void set_use_v3(bool use_v3);

    /**
     * Get the Zarr version to use.
     * 
     * @returns true if using Zarr version 3, false if using version 2
     */
    bool get_use_v3() const;

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

    /**
     * Get the URI of the Zarr file.
     * 
     * @returns the URI of the Zarr file
     */
    std::string get_uri() const;

    /**
     * Set the URI of the Zarr file.
     * 
     * @param uri the URI of the Zarr file
     */
    void set_uri(const std::string& uri);
    /**
     * Get the metadata of the Zarr file.
     * 
     * @returns the metadata of the Zarr file
     */
    std::string get_metadata() const;

    /**
     * Set the metadata of the Zarr file.
     * 
     * @param metadata the metadata of the Zarr file
     */
    void setExternalMetadata(const std::string& metadata);

    /**
     * Get the pixel scale in the x direction.
     * 
     * @returns the pixel scale in the x direction
     */
    double get_pixel_scale_x() const;

    /**
     * Set the pixel scale in the x direction.
     * 
     * @param pixel_scale_x the pixel scale in the x direction
     */
    void set_pixel_scale_x(double pixel_scale_x);

    /**
     * Get the pixel scale in the y direction.
     * 
     * @returns the pixel scale in the y direction
     */
    double get_pixel_scale_y() const;

    /**
     * Set the pixel scale in the y direction.
     * 
     * @param pixel_scale_y the pixel scale in the y direction
     */
    void set_pixel_scale_y(double pixel_scale_y);

    /**
     * Set the id of the first frame.
     */
    void set_first_frame_id(uint32_t id);

    /**
     * Get the id of the first frame.
     * 
     * @returns the id of the first frame
     */
    uint32_t get_first_frame_id() const;

    /**
     * Set the dimensions of the Zarr file.
     * 
     * @param dimensions the dimensions of the Zarr file
     * Valid dimensions are: "x", "y", "z", "c", & "t" for 
     * x, y, z, channel, and time respectively.
     */
    void set_dimensions(const std::vector< std::string >& dimensions);

    /**
     * Get the dimensions of the Zarr file.
     * 
     * @returns the dimensions of the Zarr file
     */
    std::vector< std::string > get_dimensions() const;

    void set_dimension_sizes(const std::vector< uint32_t >& dimensions);
    std::vector< uint32_t > get_dimension_sizes() const;

    void set_chunk_sizes(const std::vector< uint32_t >& chunk_sizes);
    std::vector< uint32_t > get_chunk_sizes() const;

    void set_shard_sizes(const std::vector< uint32_t >& shard_sizes);
    std::vector< uint32_t > get_shard_sizes() const;

    void set_enable_multiscale(bool multiscale);
    bool get_enable_multiscale() const;

    void set_compression_codec(AcquireZarrCompressionCodec compression);
    AcquireZarrCompressionCodec get_compression_codec() const;

    void set_compression_level(int level);
    int get_compression_level() const;

    void set_compression_shuffle(int shuffle);
    int get_compression_shuffle() const;

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
