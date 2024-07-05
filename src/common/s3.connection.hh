#ifndef H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0
#define H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace minio::s3 {
class Client;
class Part;
} // namespace minio::s3

namespace minio::creds {
class StaticProvider;
} // namespace minio::creds

namespace acquire::sink::zarr::common {
struct S3Connection final
{
    S3Connection(const S3Connection&) = delete;
    explicit S3Connection(const std::string& endpoint,
                          const std::string& access_key_id,
                          const std::string& secret_access_key);

    ~S3Connection() noexcept;

    // bucket operations
    bool bucket_exists(const std::string& bucket_name);
    [[nodiscard]] bool make_bucket(const std::string& bucket_name);
    [[nodiscard]] bool destroy_bucket(const std::string& bucket_name);

    // object operations
    bool object_exists(const std::string& bucket_name,
                       const std::string& object_name);
    [[nodiscard]] bool put_object(const std::string& bucket_name,
                                  const std::string& object_name,
                                  const uint8_t* data,
                                  size_t nbytes,
                                  std::string& etag);
    [[nodiscard]] bool delete_object(const std::string& bucket_name,
                                     const std::string& object_name);

    // multipart object operations
    [[nodiscard]] bool create_multipart_object(const std::string& bucket_name,
                                               const std::string& object_name,
                                               std::string& upload_id);
    [[nodiscard]] bool upload_multipart_object_part(
      const std::string& bucket_name,
      const std::string& object_name,
      const std::string& upload_id,
      const uint8_t* data,
      size_t nbytes,
      size_t part_number,
      std::string& etag);
    [[nodiscard]] bool complete_multipart_object(
      const std::string& bucket_name,
      const std::string& object_name,
      const std::string& upload_id,
      const std::list<minio::s3::Part>& parts);

  private:
    std::unique_ptr<minio::s3::Client> client_;
    std::unique_ptr<minio::creds::StaticProvider> provider_;
};

struct S3ConnectionPool final
{
  public:
    S3ConnectionPool(size_t n_connections,
                     const std::string& endpoint,
                     const std::string& access_key_id,
                     const std::string& secret_access_key);
    ~S3ConnectionPool() noexcept;

    std::unique_ptr<S3Connection> get_connection();
    void release_connection(std::unique_ptr<S3Connection>&& conn);

  private:
    std::vector<std::unique_ptr<S3Connection>> connections_;
    mutable std::mutex connections_mutex_;
    std::condition_variable cv_;

    std::atomic<bool> is_accepting_connections_;

    std::unique_ptr<S3Connection> pop_from_connection_pool_();
    [[nodiscard]] bool should_stop_() const;
};
} // namespace acquire::sink::zarr::common
#endif // H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0
