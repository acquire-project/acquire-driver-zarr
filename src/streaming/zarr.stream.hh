#pragma once

#include "stream.settings.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "array.writer.hh"

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr

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

    ZarrVersion version() const { return version_; }
    const ZarrStreamSettings_s& settings() const { return settings_; }

  private:
    struct ZarrStreamSettings_s settings_;
    ZarrVersion version_;   // Zarr version. 2 or 3.
    std::string error_; // error message. If nonempty, an error occurred.

    std::vector<uint8_t> frame_buffer_;
    size_t frame_buffer_offset_;

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::vector<std::unique_ptr<zarr::ArrayWriter>> writers_;
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>
      metadata_sinks_;

    std::unordered_map<size_t, std::optional<uint8_t*>> scaled_frames_;

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
