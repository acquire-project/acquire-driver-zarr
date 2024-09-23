#pragma once

#include "zarr.types.h"
#include "zarr.dimension.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "array.writer.hh"

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr
#include <optional>

struct ZarrStream_s
{
  public:
    explicit ZarrStream_s(struct ZarrStreamSettings_s* settings);
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

    std::vector<uint8_t> frame_buffer_;
    size_t frame_buffer_offset_;

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::vector<std::unique_ptr<zarr::ArrayWriter>> writers_;
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>
      metadata_sinks_;

    std::unordered_map<size_t, std::optional<uint8_t*>> scaled_frames_;

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
    [[nodiscard]] nlohmann::json make_multiscale_metadata_() const;

    void write_multiscale_frames_(const uint8_t* data, size_t bytes_of_data);
};
