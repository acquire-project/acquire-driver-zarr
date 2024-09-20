#pragma once

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr
#include <optional>

struct ZarrDimension_s
{
  public:
    ZarrDimension_s(const char* name,
                    ZarrDimensionType type,
                    uint32_t array_size_px,
                    uint32_t chunk_size_px,
                    uint32_t shard_size_chunks)
      : name(name)
      , type(type)
      , array_size_px(array_size_px)
      , chunk_size_px(chunk_size_px)
      , shard_size_chunks(shard_size_chunks)
    {
    }

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
    ZarrStream_s(struct ZarrStreamSettings_s* settings);
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

    std::optional<std::string> s3_endpoint_;
    std::optional<std::string> s3_bucket_name_;
    std::optional<std::string> s3_access_key_id_;
    std::optional<std::string> s3_secret_access_key_;

    std::string custom_metadata_;

    ZarrDataType dtype_;

    std::optional<ZarrCompressor> compressor_;
    std::optional<ZarrCompressionCodec> compression_codec_;
    std::optional<uint8_t> compression_level_;
    std::optional<uint8_t> compression_shuffle_;

    std::vector<ZarrDimension_s> dimensions_;

    bool multiscale_;

    [[nodiscard]] bool is_s3_acquisition_() const;
    [[nodiscard]] bool is_compressed_acquisition_() const;

    /**
     * @brief Copy settings to the stream.
     * @param settings Struct containing settings to copy.
     */
    void commit_settings_(const struct ZarrStreamSettings_s* settings);

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
