#include "connection.pool.hh"
#include "logger.h"

#include <aws/core/auth/AWSCredentialsProviderChain.h>

#define LOG(...) aq_logger(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) aq_logger(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Expression was false: " #e);             \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

// #define TRACE(...) LOG(__VA_ARGS__)
#define TRACE(...)

#define containerof(ptr, T, V) ((T*)(((char*)(ptr)) - offsetof(T, V)))
#define countof(e) (sizeof(e) / sizeof(*(e)))

namespace zarr = acquire::sink::zarr;

zarr::S3Connection::S3Connection(const std::string& endpoint,
                                 const std::string& access_key_id,
                                 const std::string& secret_access_key)
{
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = endpoint;
    const Aws::Auth::AWSCredentials credentials(access_key_id,
                                                secret_access_key);
    client_ = std::make_shared<Aws::S3::S3Client>(credentials, nullptr, config);
    CHECK(client_);
}

zarr::S3Connection::~S3Connection() noexcept
{
    client_.reset();
}

std::shared_ptr<Aws::S3::S3Client>
zarr::S3Connection::client() const noexcept
{
    return client_;
}

zarr::S3ConnectionPool::S3ConnectionPool(
  size_t n_connections,
  const std::string& endpoint,
  const std::string& access_key_id,
  const std::string& secret_access_key,
  std::function<void(const std::string&)>&& err)
  : error_handler_{ std::move(err) }
  , is_accepting_connections_{ true }
{
    CHECK(n_connections > 0);

    for (auto i = 0; i < n_connections; ++i) {
        connections_.emplace_back(std::make_shared<S3Connection>(
          endpoint, access_key_id, secret_access_key));
    }
}

zarr::S3ConnectionPool::~S3ConnectionPool() noexcept
{
    is_accepting_connections_ = false;
    cv_.notify_all();
}

std::shared_ptr<zarr::S3Connection>
zarr::S3ConnectionPool::get_connection() noexcept
{
    std::unique_lock lock(connections_mutex_);
    cv_.wait(lock, [this] { return !connections_.empty(); });

    if (should_stop_()) {
        return nullptr;
    }

    auto conn = pop_from_connection_pool_();
    return conn;
}

void
zarr::S3ConnectionPool::release_connection(
  std::shared_ptr<zarr::S3Connection>&& conn) noexcept
{
    std::scoped_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
}

std::shared_ptr<zarr::S3Connection>
zarr::S3ConnectionPool::pop_from_connection_pool_() noexcept
{
    if (connections_.empty()) {
        return nullptr;
    }

    auto conn = connections_.back();
    connections_.pop_back();
    return conn;
}

bool
zarr::S3ConnectionPool::should_stop_() const noexcept
{
    return !is_accepting_connections_;
}
