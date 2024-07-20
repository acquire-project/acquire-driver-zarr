#pragma once

#include "sink.hh"
#include "platform.h"
#include "common/s3.connection.hh"

#include <miniocpp/types.h>

#include <array>
#include <string>

namespace acquire::sink::zarr {
class S3Sink final : public Sink
{
  public:
    S3Sink() = delete;
    S3Sink(std::string_view bucket_name,
           std::string_view object_key,
           std::shared_ptr<common::S3ConnectionPool> connection_pool);
    ~S3Sink() override;

    bool write(size_t offset,
               const uint8_t* data,
               size_t bytes_of_data) override;

  private:
    std::string bucket_name_;
    std::string object_key_;

    std::shared_ptr<common::S3ConnectionPool> connection_pool_;

    // multipart upload
    std::array<uint8_t, 5 << 20> part_buffer_; /// temporary 5MiB buffer for multipart upload
    size_t n_bytes_buffered_ = 0;

    std::string upload_id_;
    std::list<minio::s3::Part> parts_;

    // single-part upload
    /// @brief Upload the object to S3.
    /// @returns True if the object was successfully uploaded, otherwise false.
    [[nodiscard]] bool put_object_();

    // multipart upload
    bool is_multipart_upload_() const;

    /// @brief Get the multipart upload ID, if it exists. Otherwise, create a new
    /// multipart upload.
    /// @returns The multipart upload ID.
    std::string get_multipart_upload_id_();

    /// @brief Flush the current part to S3.
    /// @returns True if the part was successfully flushed, otherwise false.
    [[nodiscard]] bool flush_part_();
    /// @brief Finalize the multipart upload.
    /// @returns True if a multipart upload was successfully finalized,
    /// otherwise false.
    [[nodiscard]] bool finalize_multipart_upload_();
};
} // namespace acquire::sink::zarr
