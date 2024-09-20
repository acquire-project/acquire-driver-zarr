#pragma once

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr

struct ZarrDimension_s
{
    std::string name;       /* Name of the dimension */
    ZarrDimensionType type; /* Type of dimension */

    uint32_t array_size_px;     /* Size of the array along this dimension */
    uint32_t chunk_size_px;     /* Size of a chunk along this dimension */
    uint32_t shard_size_chunks; /* Number of chunks in a shard along this
                                 * dimension */
};

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings, ZarrVersion version);
    ~ZarrStream_s();

    /**
     * @brief Append data to the stream.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append(const void* data, size_t nbytes);

  private:
    std::string error_; // error message. If nonempty, an error occurred.

    ZarrVersion version_;

    std::string store_path_;

    std::string s3_endpoint;
    std::string s3_bucket_name;
    std::string s3_access_key_id;
    std::string s3_secret_access_key;

    std::string custom_metadata;

    ZarrDataType dtype;

    ZarrCompressor compressor;
    ZarrCompressionCodec compression_codec;
    uint8_t compression_level;
    uint8_t compression_shuffle;

    std::vector<ZarrDimension_s> dimensions;

    bool multiscale;

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    /** @brief Create the data store. */
    [[nodiscard]] bool create_store_();

    /** @brief Create the writers. */
    [[nodiscard]] bool create_writers_();

    /** @brief Create placeholders for multiscale frames. */
    void create_scaled_frames_();

    /** @brief Create the metadata sinks. */
    [[nodiscard]] bool create_metadata_sinks_();

    /** @brief Write per-acquisition metadata. */
    [[nodiscard]] bool write_base_metadata_();

    /** @brief Write Zarr group metadata. */
    bool write_group_metadata_();

    /** @brief Write external metadata. */
    [[nodiscard]] bool write_external_metadata_();

    /** @brief Construct OME metadata pertaining to the multiscale pyramid. */
    nlohmann::json make_multiscale_metadata_() const;

    void write_multiscale_frames_(const uint8_t* data, size_t bytes_of_data);
};
