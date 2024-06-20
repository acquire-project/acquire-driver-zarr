#ifndef H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0
#define H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0

#include <miniocpp/client.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace acquire::sink::zarr {
struct S3Connection final
{
    S3Connection(const S3Connection&) = delete;
    explicit S3Connection(const std::string& endpoint,
                          const std::string& access_key_id,
                          const std::string& secret_access_key);

    ~S3Connection() noexcept;

//    std::unique_ptr<minio::s3::Client> client() const noexcept;

  private:
    std::unique_ptr<minio::s3::Client> client_;
};

struct S3ConnectionPool final
{
  public:
    S3ConnectionPool(size_t n_connections,
                     const std::string& endpoint,
                     const std::string& access_key_id,
                     const std::string& secret_access_key);
    ~S3ConnectionPool() noexcept;

    std::unique_ptr<S3Connection> get_connection() noexcept;
    void release_connection(std::unique_ptr<S3Connection>&& conn) noexcept;

  private:
    std::vector<std::unique_ptr<S3Connection>> connections_;
    mutable std::mutex connections_mutex_;
    std::condition_variable cv_;

    std::atomic<bool> is_accepting_connections_;

    std::unique_ptr<S3Connection> pop_from_connection_pool_() noexcept;
    [[nodiscard]] bool should_stop_() const noexcept;
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_STORAGE_ZARR_S3_CONNECTION_POOL_V0
