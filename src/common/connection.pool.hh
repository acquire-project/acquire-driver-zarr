#ifndef H_ACQUIRE_STORAGE_ZARR_CONNECTION_POOL_V0
#define H_ACQUIRE_STORAGE_ZARR_CONNECTION_POOL_V0

#include <aws/s3/S3Client.h>

#include <memory>

namespace acquire::sink::zarr {
struct S3Connection final
{
    explicit S3Connection(const std::string& endpoint,
                          const std::string& access_key_id,
                          const std::string& secret_access_key);
    ~S3Connection() noexcept;

  private:
    std::unique_ptr<Aws::S3::S3Client> client_;
};

struct S3ConnectionPool final
{
  public:
    S3ConnectionPool(size_t n_connections,
                     const std::string& endpoint,
                     const std::string& access_key_id,
                     const std::string& secret_access_key,
                     std::function<void(const std::string&)>&& err);
    ~S3ConnectionPool() noexcept;

    std::shared_ptr<S3Connection> get_connection() noexcept;
    void release_connection(std::shared_ptr<S3Connection>&& conn) noexcept;

  private:
    std::function<void(const std::string&)> error_handler_;

    std::vector<std::shared_ptr<S3Connection>> connections_;
    mutable std::mutex connections_mutex_;
    std::condition_variable cv_;

    std::atomic<bool> is_accepting_connections_;

    std::shared_ptr<S3Connection> pop_from_connection_pool_() noexcept;
    [[nodiscard]] bool should_stop_() const noexcept;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_CONNECTION_POOL_V0
