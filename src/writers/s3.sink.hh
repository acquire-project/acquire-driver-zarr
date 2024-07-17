#pragma once

#include "sink.hh"
#include "platform.h"
#include "common/s3.connection.hh"

#include <miniocpp/types.h>

#include <future>
#include <string>

namespace acquire::sink::zarr {
struct S3Sink final : public Sink
{
    S3Sink() = delete;
    S3Sink(const std::string& bucket_name,
           const std::string& object_key,
           std::shared_ptr<common::S3ConnectionPool> connection_pool);
    ~S3Sink() override;

    bool write(size_t offset, const uint8_t* buf, size_t bytes_of_buf) override;

  private:
    std::string bucket_name_;
    std::string object_key_;

    std::shared_ptr<common::S3ConnectionPool> connection_pool_;

    // multipart upload
    std::vector<uint8_t> buf_; // temporary 5MiB buffer for multipart upload
    size_t buf_size_ = 0;

    std::string upload_id_;
    std::list<minio::s3::Part> parts_;

    // single-part upload
    /// @brief Upload the object to S3.
    /// @returns True if the object was successfully uploaded, otherwise false.
    [[nodiscard]] bool put_object_() noexcept;

    // multipart upload
    /// @brief Flush the current part to S3.
    /// @returns True if the part was successfully flushed, otherwise false.
    [[nodiscard]] bool flush_part_() noexcept;
    /// @brief Finalize the multipart upload.
    /// @returns True if a multipart upload was successfully finalized, otherwise false.
    [[nodiscard]] bool finalize_multipart_upload_() noexcept;
};
} // namespace acquire::sink::zarr
