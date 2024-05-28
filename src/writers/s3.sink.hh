#ifndef H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0

#include "sink.hh"
#include "platform.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct S3Sink : public Sink
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
    ~S3Sink() override = default;

    [[nodiscard]] bool write(size_t offset,
                             const uint8_t* buf,
                             size_t bytes_of_buf) override;

  protected:
    S3Sink::Config config_;
    std::unique_ptr<Aws::S3::S3Client> s3_client_;
    std::string upload_id_;
};

struct S3MultipartSink : public S3Sink
{
    explicit S3MultipartSink(S3Sink::Config&& config);
    ~S3MultipartSink() override;

    [[nodiscard]] bool write(size_t offset,
                             const uint8_t* buf,
                             size_t bytes_of_buf) override;

  private:
    std::vector<Aws::S3::Model::CompletedPart> completed_parts_;

    void close_();
};

struct S3SinkCreator
{
  public:
    S3SinkCreator() = delete;
    explicit S3SinkCreator(std::shared_ptr<common::ThreadPool> thread_pool,
                           const std::string& endpoint,
                           const std::string& bucket_name,
                           const std::string& access_key_id,
                           const std::string& secret_access_key);
    ~S3SinkCreator() noexcept = default;

    [[nodiscard]] bool create_chunk_sinks(
      const std::string& data_root,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& chunk_sinks,
      size_t chunk_size_bytes);

    [[nodiscard]] bool create_shard_sinks(
      const std::string& data_root,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& shard_sinks,
      size_t shard_size_bytes);

    [[nodiscard]] bool create_metadata_sinks(
      const std::vector<std::string>& paths,
      std::vector<Sink*>& metadata_sinks);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::string endpoint_;
    std::string bucket_name_;
    std::string access_key_id_;
    std::string secret_access_key_;

    /// @brief Parallel create a collection of S3 objects.
    /// @param[in,out] paths Paths to S3 objects to create.
    /// @param[out] sinks Sink representations of objects created.
    /// @return True iff all S3 sinks were created successfully.
    [[nodiscard]] bool make_s3_objects_(std::queue<std::string>& paths,
                                        std::vector<Sink*>& sinks,
                                        bool multipart);
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
