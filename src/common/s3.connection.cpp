#include "s3.connection.hh"
#include "utilities.hh"

#include <miniocpp/client.h>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

common::S3Connection::S3Connection(const std::string& endpoint,
                                   const std::string& access_key_id,
                                   const std::string& secret_access_key)
{
    minio::s3::BaseUrl url(endpoint);

    provider_ = std::make_unique<minio::creds::StaticProvider>(
      access_key_id, secret_access_key);
    client_ = std::make_unique<minio::s3::Client>(url, provider_.get());
    CHECK(client_);
}

common::S3Connection::~S3Connection() noexcept
{
    client_.reset();
}

bool
common::S3Connection::bucket_exists(const std::string& bucket_name) noexcept
{
    if (bucket_name.empty()) {
        return false;
    }

    try {
        minio::s3::BucketExistsArgs args;
        args.bucket = bucket_name;

        minio::s3::BucketExistsResponse response = client_->BucketExists(args);
        CHECK(response);

        return response.exist;
    } catch (const std::exception& e) {
        LOGE("Failed to list buckets: %s", e.what());
    }

    return false;
}

bool
common::S3Connection::make_bucket(const std::string& bucket_name) noexcept
{
    if (bucket_name.empty()) {
        return false;
    }

    // first check if the bucket exists, do nothing if it does
    if (bucket_exists(bucket_name)) {
        return true;
    }

    TRACE("Creating bucket %s", bucket_name.c_str());
    minio::s3::MakeBucketArgs args;
    args.bucket = bucket_name;

    try {
        minio::s3::MakeBucketResponse response = client_->MakeBucket(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to create bucket %s: %s", bucket_name.c_str(), e.what());
    }

    return false;
}

bool
common::S3Connection::destroy_bucket(const std::string& bucket_name) noexcept
{
    if (bucket_name.empty()) {
        return false;
    }

    // first check if the bucket exists, do nothing if it does
    if (!bucket_exists(bucket_name)) {
        return true;
    }

    TRACE("Destroying bucket %s", bucket_name.c_str());
    minio::s3::RemoveBucketArgs args;
    args.bucket = bucket_name;

    try {
        minio::s3::RemoveBucketResponse response = client_->RemoveBucket(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to destroy bucket %s: %s", bucket_name.c_str(), e.what());
    }

    return false;
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

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

#if __has_include("s3.credentials.hpp")
#include "s3.credentials.hpp"
static std::string s3_endpoint = ZARR_S3_ENDPOINT;
static std::string s3_access_key_id = ZARR_S3_ACCESS_KEY_ID;
static std::string s3_secret_access_key = ZARR_S3_SECRET_ACCESS_KEY;
#else
static std::string s3_endpoint, s3_access_key_id, s3_secret_access_key;
#endif

extern "C"
{
    acquire_export int unit_test__make_s3_bucket()
    {
        if (s3_endpoint.empty() || s3_access_key_id.empty() ||
            s3_secret_access_key.empty()) {
            LOGE("S3 credentials not set.");
            return 1;
        }

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (conn.bucket_exists("acquire-test-bucket")) {
                CHECK(conn.destroy_bucket("acquire-test-bucket"));
            }

            CHECK(conn.make_bucket("acquire-test-bucket"));
            CHECK(conn.bucket_exists("acquire-test-bucket"));
            CHECK(conn.destroy_bucket("acquire-test-bucket"));

            retval = 1;
        } catch (const std::exception& e) {
            LOGE("Failed to create S3 connection: %s", e.what());
        } catch (...) {
            LOGE("Failed to create S3 connection: unknown error");
        }

        return retval;
    }
}
#endif
