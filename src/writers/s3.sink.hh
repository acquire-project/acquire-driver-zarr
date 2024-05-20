#ifndef H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0

#include "sink.hh"
#include "platform.h"
#include <aws/s3/S3Client.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct S3Sink : public Sink
{
    struct Config
    {
        std::string uri;
        std::string access_key_id;
        std::string secret_access_key;
    };

    explicit S3Sink(S3Sink::Config&& config);
    ~S3Sink() override;

    [[nodiscard]] bool write(size_t offset,
                             const uint8_t* buf,
                             size_t bytes_of_buf) override;

  private:
    S3Sink::Config config_;
//    Aws::Client::ClientConfiguration client_config_;
//    Aws::S3::S3Client s3_client_;
    std::string upload_id_;

    void close_();
};

struct S3SinkCreator
{
  public:
    S3SinkCreator() = delete;
    explicit S3SinkCreator(std::shared_ptr<common::ThreadPool> thread_pool,
                           const std::string& access_key_id,
                           const std::string& secret_access_key);
    ~S3SinkCreator() noexcept = default;

    [[nodiscard]] bool create_chunk_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& chunk_sinks);

    [[nodiscard]] bool create_shard_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& shard_sinks);

    [[nodiscard]] bool create_metadata_sinks(
      const std::vector<std::string>& paths,
      std::vector<Sink*>& metadata_sinks);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::string access_key_id_;
    std::string secret_access_key_;

    /// @brief Parallel create a collection of directories.
    /// @param[in] dir_paths The directories to create.
    /// @return True iff all directories were created successfully.
    //    [[nodiscard]] bool make_dirs_(std::queue<fs::path>& dir_paths);

    /// @brief Parallel create a collection of files.
    /// @param[in,out] file_paths The files to create. Unlike `make_dirs_`,
    /// this function drains the queue.
    /// @param[out] files The files created.
    /// @return True iff all files were created successfully.
    //    [[nodiscard]] bool make_files_(std::queue<fs::path>& file_paths,
    //                                   std::vector<Sink*>& files);
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_S3_SINK_V0
