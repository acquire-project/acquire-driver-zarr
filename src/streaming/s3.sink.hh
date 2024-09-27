#pragma once

#include "sink.hh"
#include "s3.connection.hh"

#include <miniocpp/types.h>

#include <array>
#include <string>

namespace zarr {
class S3Sink : public Sink
{
  public:
    S3Sink(std::string_view bucket_name,
           std::string_view object_key,
           std::shared_ptr<S3ConnectionPool> connection_pool);
    ~S3Sink() override;

    bool write(size_t offset,
               const uint8_t* data,
               size_t bytes_of_data) override;

  private:
    std::string bucket_name_;
    std::string object_key_;

    std::shared_ptr<S3ConnectionPool> connection_pool_;

    // multipart upload
    std::array<uint8_t, 5 << 20> part_buffer_;
    size_t nbytes_buffered_ = 0;
    size_t nbytes_flushed_ = 0;

    std::string upload_id_;
    std::list<minio::s3::Part> parts_;

    // single-part upload
    /// @brief Upload the object to S3.
    /// @returns True if the object was successfully uploaded, otherwise false.
    [[nodiscard]] bool put_object_();

    // multipart upload
    bool is_multipart_upload_() const;

    /// @brief Get the multipart upload ID, if it exists. Otherwise, create a
    /// new multipart upload.
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
} // namespace zarr
