#ifndef H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0

#include "platform.h"

#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct S3Sink
{
    struct Config
    {
        std::string endpoint;
        std::string bucket_name;
        std::string object_key;
        std::string access_key_id;
        std::string secret_access_key;
    };

    explicit S3Sink(S3Sink::Config&& config);
    ~S3Sink();

    [[nodiscard]] bool write(const uint8_t* buf, size_t bytes_of_buf);
    [[nodiscard]] bool write_multipart(const uint8_t* buf, size_t bytes_of_buf);
    void close();

  private:
    using UploadPartResultOutcome =
      Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>;

    S3Sink::Config config_;
    std::unique_ptr<Aws::S3::S3Client> s3_client_;

    // multipart only
    std::string upload_id_;
    std::vector<std::future<UploadPartResultOutcome>> callables_;
};

//struct S3SinkCreator
//{
//  public:
//    S3SinkCreator() = delete;
//    explicit S3SinkCreator(std::shared_ptr<common::ThreadPool> thread_pool,
//                           const std::string& endpoint,
//                           const std::string& bucket_name,
//                           const std::string& access_key_id,
//                           const std::string& secret_access_key);
//    ~S3SinkCreator() noexcept = default;
//
//    [[nodiscard]] bool create_chunk_sinks(
//      const std::string& data_root,
//      const std::vector<Dimension>& dimensions,
//      std::vector<Sink*>& chunk_sinks,
//      size_t chunk_size_bytes);
//
//    [[nodiscard]] bool create_shard_sinks(
//      const std::string& data_root,
//      const std::vector<Dimension>& dimensions,
//      std::vector<Sink*>& shard_sinks,
//      size_t shard_size_bytes);
//
//    [[nodiscard]] bool create_metadata_sinks(
//      const std::vector<std::string>& paths,
//      std::vector<Sink*>& metadata_sinks);
//
//  private:
//    std::shared_ptr<common::ThreadPool> thread_pool_;
//    std::string endpoint_;
//    std::string bucket_name_;
//    std::string access_key_id_;
//    std::string secret_access_key_;
//
//    /// @brief Parallel create a collection of S3 objects.
//    /// @param[in,out] paths Paths to S3 objects to create.
//    /// @param[out] sinks Sink representations of objects created.
//    /// @return True iff all S3 sinks were created successfully.
//    [[nodiscard]] bool make_s3_objects_(std::queue<std::string>& paths,
//                                        std::vector<Sink*>& sinks,
//                                        bool multipart);
//};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
