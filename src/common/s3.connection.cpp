#include "s3.connection.hh"
#include "utilities.hh"

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

common::S3Connection::S3Connection(const std::string& endpoint,
                                   const std::string& access_key_id,
                                   const std::string& secret_access_key)
{
    minio::s3::BaseUrl url(endpoint);
    minio::creds::StaticProvider provider(access_key_id, secret_access_key);

    client_ = std::make_unique<minio::s3::Client>(url, &provider);
    CHECK(client_);
}

common::S3Connection::~S3Connection() noexcept
{
    client_.reset();
}

bool
common::S3Connection::make_bucket(const std::string& bucket_name) noexcept
{
    // first check if the bucket exists, do nothing if it does
    try {
        minio::s3::ListBucketsResponse response = client_->ListBuckets();
        CHECK(response);

        for (const auto& bucket : response.buckets) {
            if (bucket.name == bucket_name) {
                return true;
            }
        }
    } catch (const std::exception& e) {
        LOGE("Failed to list buckets: %s", e.what());
        return false;
    }

    TRACE("Creating bucket %s", bucket_name.c_str());
    minio::s3::MakeBucketArgs args;
    args.bucket = bucket_name;

    try {
        minio::s3::MakeBucketResponse response = client_->MakeBucket(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to create bucket %s: %s", bucket_name.c_str(), e.what());
        return false;
    }
}

common::S3ConnectionPool::S3ConnectionPool(size_t n_connections,
                                           const std::string& endpoint,
                                           const std::string& access_key_id,
                                           const std::string& secret_access_key)
  : is_accepting_connections_{ true }
{
    CHECK(n_connections > 0);

    for (auto i = 0; i < n_connections; ++i) {
        connections_.push_back(std::make_unique<S3Connection>(
          endpoint, access_key_id, secret_access_key));
    }
}

common::S3ConnectionPool::~S3ConnectionPool() noexcept
{
    is_accepting_connections_ = false;
    cv_.notify_all();
}

std::unique_ptr<common::S3Connection>
common::S3ConnectionPool::get_connection() noexcept
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
common::S3ConnectionPool::release_connection(
  std::unique_ptr<common::S3Connection>&& conn) noexcept
{
    std::scoped_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
}

std::unique_ptr<common::S3Connection>
common::S3ConnectionPool::pop_from_connection_pool_() noexcept
{
    if (connections_.empty()) {
        return nullptr;
    }

    auto conn = std::move(connections_.back());
    connections_.pop_back();
    return conn;
}

bool
common::S3ConnectionPool::should_stop_() const noexcept
{
    return !is_accepting_connections_;
}