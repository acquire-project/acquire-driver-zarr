#include "s3.connection.hh"
#include "utilities.hh"

#include <miniocpp/client.h>

#include <list>
#include <sstream>
#include <string_view>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

common::S3Connection::S3Connection(const std::string& endpoint,
                                   const std::string& access_key_id,
                                   const std::string& secret_access_key)
{
    minio::s3::BaseUrl url(endpoint);
    url.https = endpoint.starts_with("https");

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
common::S3Connection::bucket_exists(const std::string& bucket_name)
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
        LOGE("Failed to check existence of bucket: %s", e.what());
    }

    return false;
}

bool
common::S3Connection::make_bucket(const std::string& bucket_name)
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
common::S3Connection::destroy_bucket(const std::string& bucket_name)
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

bool
common::S3Connection::object_exists(const std::string& bucket_name,
                                    const std::string& object_name)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    try {
        minio::s3::StatObjectArgs args;
        args.bucket = bucket_name;
        args.object = object_name;

        minio::s3::StatObjectResponse response = client_->StatObject(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to check if object %s exists in bucket %s: %s",
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
    }

    return false;
}

bool
common::S3Connection::put_object(const std::string& bucket_name,
                                 const std::string& object_name,
                                 const uint8_t* data,
                                 size_t nbytes,
                                 std::string& etag)
{
    if (bucket_name.empty() || object_name.empty() || 0 == nbytes) {
        return false;
    }

    std::stringstream ss;
    std::copy(reinterpret_cast<const char*>(data),
              reinterpret_cast<const char*>(data) + nbytes,
              std::ostream_iterator<char>(ss));

    TRACE("Putting object %s in bucket %s",
          object_name.c_str(),
          bucket_name.c_str());
    minio::s3::PutObjectArgs args(ss, (long)nbytes, 0);
    args.bucket = bucket_name;
    args.object = object_name;

    try {
        minio::s3::PutObjectResponse response = client_->PutObject(args);
        etag = response.etag;

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to put object %s in bucket %s: %s",
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
    }

    return false;
}

bool
common::S3Connection::delete_object(const std::string& bucket_name,
                                    const std::string& object_name)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    TRACE("Deleting object %s from bucket %s",
          object_name.c_str(),
          bucket_name.c_str());
    minio::s3::RemoveObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    try {
        minio::s3::RemoveObjectResponse response = client_->RemoveObject(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to delete object %s from bucket %s: %s",
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
    }

    return false;
}

bool
common::S3Connection::create_multipart_object(const std::string& bucket_name,
                                              const std::string& object_name,
                                              std::string& upload_id)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    TRACE("Creating multipart object %s in bucket %s",
          object_name.c_str(),
          bucket_name.c_str());
    minio::s3::CreateMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    try {
        minio::s3::CreateMultipartUploadResponse response =
          client_->CreateMultipartUpload(args);

        upload_id = response.upload_id;
        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to create multipart object %s in bucket %s: %s",
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
    }

    return false;
}

bool
common::S3Connection::upload_multipart_object_part(
  const std::string& bucket_name,
  const std::string& object_name,
  const std::string& upload_id,
  const uint8_t* data,
  size_t nbytes,
  size_t part_number,
  std::string& etag)
{
    if (bucket_name.empty() || object_name.empty() || 0 == nbytes) {
        return false;
    }

    TRACE("Uploading multipart object part %zu for object %s in bucket %s",
          part_number,
          object_name.c_str(),
          bucket_name.c_str());

    std::string_view sv(reinterpret_cast<const char*>(data), nbytes);

    minio::s3::UploadPartArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.part_number = part_number;
    args.upload_id = upload_id;
    args.data = sv;

    try {
        minio::s3::UploadPartResponse response = client_->UploadPart(args);
        etag = response.etag;

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to upload multipart object part %zu for object %s in "
             "bucket %s: %s",
             part_number,
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
    }

    return false;
}

bool
common::S3Connection::complete_multipart_object(
  const std::string& bucket_name,
  const std::string& object_name,
  const std::string& upload_id,
  const std::list<minio::s3::Part>& parts)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    TRACE("Completing multipart object %s in bucket %s",
          object_name.c_str(),
          bucket_name.c_str());
    minio::s3::CompleteMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.upload_id = upload_id;
    args.parts = parts;

    try {
        minio::s3::CompleteMultipartUploadResponse response =
          client_->CompleteMultipartUpload(args);

        return (bool)response;
    } catch (const std::exception& e) {
        LOGE("Failed to complete multipart object %s in bucket %s: %s",
             object_name.c_str(),
             bucket_name.c_str(),
             e.what());
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
common::S3ConnectionPool::get_connection()
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
  std::unique_ptr<S3Connection>&& conn)
{
    std::scoped_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
}

std::unique_ptr<common::S3Connection>
common::S3ConnectionPool::pop_from_connection_pool_()
{
    if (connections_.empty()) {
        return nullptr;
    }

    auto conn = std::move(connections_.back());
    connections_.pop_back();
    return conn;
}

bool
common::S3ConnectionPool::should_stop_() const
{
    return !is_accepting_connections_;
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

#if __has_include("s3.credentials.hh")
#include "s3.credentials.hh"
static std::string s3_endpoint = ZARR_S3_ENDPOINT;
static std::string s3_access_key_id = ZARR_S3_ACCESS_KEY_ID;
static std::string s3_secret_access_key = ZARR_S3_SECRET_ACCESS_KEY;
#else
static std::string s3_endpoint, s3_access_key_id, s3_secret_access_key;
#endif

extern "C"
{
    acquire_export int unit_test__s3_connection__make_bucket()
    {
        if (s3_endpoint.empty() || s3_access_key_id.empty() ||
            s3_secret_access_key.empty()) {
            LOGE("S3 credentials not set.");
            return 1;
        }

        const std::string bucket_name = "acquire-test-bucket";

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (conn.bucket_exists(bucket_name)) {
                CHECK(conn.destroy_bucket(bucket_name));
            }

            CHECK(conn.make_bucket(bucket_name));
            CHECK(conn.bucket_exists(bucket_name));
            CHECK(conn.destroy_bucket(bucket_name));

            retval = 1;
        } catch (const std::exception& e) {
            LOGE("Failed to create S3 connection: %s", e.what());
        } catch (...) {
            LOGE("Failed to create S3 connection: unknown error");
        }

        return retval;
    }

    acquire_export int unit_test__s3_connection__put_object()
    {
        if (s3_endpoint.empty() || s3_access_key_id.empty() ||
            s3_secret_access_key.empty()) {
            LOGE("S3 credentials not set.");
            return 1;
        }

        const std::string bucket_name = "acquire-test-bucket";
        const std::string object_name = "test-object";

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (!conn.bucket_exists(bucket_name)) {
                CHECK(conn.make_bucket(bucket_name));
                CHECK(conn.bucket_exists(bucket_name));
            }

            if (conn.object_exists(bucket_name, object_name)) {
                CHECK(conn.delete_object(bucket_name, object_name));
                CHECK(!conn.object_exists(bucket_name, object_name));
            }

            std::string etag;

            std::vector<uint8_t> data(1024, 0);
            CHECK(conn.put_object(
              bucket_name, object_name, data.data(), data.size(), etag));

            CHECK(!etag.empty());
            CHECK(conn.object_exists(bucket_name, object_name));

            // cleanup
            CHECK(conn.delete_object(bucket_name, object_name));
            CHECK(conn.destroy_bucket(bucket_name));

            retval = 1;
        } catch (const std::exception& e) {
            LOGE("Failed to create S3 connection: %s", e.what());
        } catch (...) {
            LOGE("Failed to create S3 connection: unknown error");
        }

        return retval;
    }

    acquire_export int unit_test__s3_connection__upload_multipart_object()
    {
        if (s3_endpoint.empty() || s3_access_key_id.empty() ||
            s3_secret_access_key.empty()) {
            LOGE("S3 credentials not set.");
            return 1;
        }

        const std::string bucket_name = "acquire-test-bucket";
        const std::string object_name = "test-object";

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (!conn.bucket_exists(bucket_name)) {
                CHECK(conn.make_bucket(bucket_name));
                CHECK(conn.bucket_exists(bucket_name));
            }

            if (conn.object_exists(bucket_name, object_name)) {
                CHECK(conn.delete_object(bucket_name, object_name));
                CHECK(!conn.object_exists(bucket_name, object_name));
            }

            std::string upload_id;
            CHECK(conn.create_multipart_object(
              bucket_name, object_name, upload_id));

            std::list<minio::s3::Part> parts;

            // parts need to be at least 5MiB, except the last part
            std::vector<uint8_t> data(5 << 20, 0);
            for (auto i = 0; i < 4; ++i) {
                std::string etag;
                CHECK(conn.upload_multipart_object_part(bucket_name,
                                                        object_name,
                                                        upload_id,
                                                        data.data(),
                                                        data.size(),
                                                        i + 1,
                                                        etag));

                minio::s3::Part part;
                part.number = i + 1;
                part.etag = etag;
                part.size = data.size();

                parts.push_back(part);
            }

            // last part is 1MiB
            {
                const unsigned int part_number = parts.size() + 1;
                const size_t part_size = 1 << 20; // 1MiB
                std::string etag;

                CHECK(conn.upload_multipart_object_part(bucket_name,
                                                        object_name,
                                                        upload_id,
                                                        data.data(),
                                                        part_size,
                                                        part_number,
                                                        etag));

                minio::s3::Part part;
                part.number = part_number;
                part.etag = etag;
                part.size = part_size;

                parts.push_back(part);
            }

            CHECK(conn.complete_multipart_object(
              bucket_name, object_name, upload_id, parts));

            CHECK(conn.object_exists(bucket_name, object_name));

            // cleanup
            CHECK(conn.delete_object(bucket_name, object_name));
            CHECK(conn.destroy_bucket(bucket_name));

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
