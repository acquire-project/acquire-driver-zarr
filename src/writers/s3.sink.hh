#ifndef H_ACQUIRE_STORAGE_ZARR_WRITERS_S3_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITERS_S3_SINK_V0

#include "sink.hh"
#include "platform.h"
#include "common/s3.connection.hh"

// #include <aws/core/utils/Outcome.h>
// #include <aws/s3/model/UploadPartResult.h>
// #include <aws/s3/S3Errors.h>

#include <future>
#include <string>

// using UploadPartResultOutcome =
//   Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>;

namespace acquire::sink::zarr {
struct S3Sink final : public Sink
{
    S3Sink() = delete;
    S3Sink(const std::string& bucket_name,
           const std::string& object_key,
           std::shared_ptr<common::S3ConnectionPool> connection_pool);
    ~S3Sink() override = default;

    bool write(size_t offset, const uint8_t* buf, size_t bytes_of_buf) override;

  private:
    std::string bucket_name_;
    std::string object_key_;

    std::shared_ptr<common::S3ConnectionPool> connection_pool_;

    // multipart upload
    std::vector<uint8_t> buf_; // temporary 5MiB buffer for multipart upload
    size_t buf_size_ = 0;

    std::string upload_id_;
    //    std::vector<std::future<UploadPartResultOutcome>> callables_;

    [[nodiscard]] bool write_to_buffer_(const uint8_t* buf,
                                        size_t bytes_of_buf);

    // single-part upload
    [[nodiscard]] bool put_object_();

    // multipart upload
    [[nodiscard]] bool flush_part_();
    [[nodiscard]] bool finalize_multipart_upload_();

    void close_();
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_WRITERS_S3_SINK_V0
