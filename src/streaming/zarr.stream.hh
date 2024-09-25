#pragma once

#include "zarr.types.h"

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr
#include <optional>

struct ZarrDimension_s
{
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

    std::string name;
    ZarrDimensionType type;

    uint32_t array_size_px;
    uint32_t chunk_size_px;
    uint32_t shard_size_chunks;
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
    struct S3Settings {
        std::string endpoint;
        std::string bucket_name;
        std::string access_key_id;
        std::string secret_access_key;
    };
    struct CompressionSettings {
        ZarrCompressor compressor;
        ZarrCompressionCodec codec;
        uint8_t level;
        uint8_t shuffle;
    };

    std::string error_; // error message. If nonempty, an error occurred.

    ZarrVersion version_;
    std::string store_path_;
    std::optional<S3Settings> s3_settings_;
    std::optional<CompressionSettings> compression_settings_;
    std::string custom_metadata_;
    ZarrDataType dtype_;
    std::vector<ZarrDimension_s> dimensions_;
    bool multiscale_;

    bool is_s3_acquisition_() const;
    bool is_compressed_acquisition_() const;

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
