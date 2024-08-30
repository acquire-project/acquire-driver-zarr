#pragma once

#include "stream.settings.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "array.writer.hh"

#include <cstddef> // size_t
#include <memory>  // unique_ptr

namespace zarr {
/**
 * @brief Get the number of bytes for a given data type.
 * @param data_type The data type.
 * @return The number of bytes for the data type.
 */
size_t
bytes_of_type(ZarrDataType data_type);

/**
 * @brief Get the number of bytes for a frame with the given dimensions and
 * data type.
 * @param dims The dimensions of the full array.
 * @param type The data type of the array.
 * @return The number of bytes for a single frame.
 */
size_t
bytes_of_frame(const std::vector<ZarrDimension_s>& dims, ZarrDataType type);
} // namespace

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings, uint8_t version);
    ~ZarrStream_s();

    /**
     * @brief Append data to the stream.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append(const void* data, size_t nbytes);

    size_t version() const { return version_; }
    const ZarrStreamSettings_s& settings() const { return settings_; }

  private:
    struct ZarrStreamSettings_s settings_;
    uint8_t version_;   // Zarr version. 2 or 3.
    std::string error_; // error message. If nonempty, an error occurred.

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::vector<std::unique_ptr<zarr::ArrayWriter>> writers_;
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>
      metadata_sinks_;

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    /** @brief Create the data store. */
    [[nodiscard]] bool create_store_();

    /** @brief Create the writers. */
    [[nodiscard]] bool create_writers_();

    /** @brief Create the metadata sinks. */
    [[nodiscard]] bool create_metadata_sinks_();

    /** @brief Write per-acquisition metadata. */
    [[nodiscard]] bool write_base_metadata_();

    /** @brief Write Zarr group metadata. */
    bool write_group_metadata_();

    /** @brief Construct OME metadata pertaining to the multiscale pyramid. */
    nlohmann::json make_multiscale_metadata_() const;
};
