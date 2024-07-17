#include "s3.connection.hh"
#include "utilities.hh"

#include <miniocpp/client.h>
#include <miniocpp/utils.h>

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

bool
common::S3Connection::check_connection()
{
    return (bool)client_->ListBuckets();
}

bool
common::S3Connection::bucket_exists(std::string_view bucket_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");

    minio::s3::BucketExistsArgs args;
    args.bucket = bucket_name;

    auto response = client_->BucketExists(args);
    return response.exist;
}

bool
common::S3Connection::object_exists(std::string_view bucket_name,
                                    std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    minio::s3::StatObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->StatObject(args);
    // casts to true if response code in 200 range and error message is empty
    return static_cast<bool>(response);
}

std::string
common::S3Connection::put_object(std::string_view bucket_name,
                                 std::string_view object_name,
                                 std::span<uint8_t> data)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Data must not be empty.");

    minio::utils::CharBuffer buffer((char*)const_cast<uint8_t*>(data.data()),
                                    data.size());
    std::basic_istream stream(&buffer);

    TRACE(
      "Putting object %s in bucket %s", object_name.data(), bucket_name.data());
    minio::s3::PutObjectArgs args(stream, (long)data.size(), 0);
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->PutObject(args);
    if (!response) {
        LOGE("Failed to put object %s in bucket %s: %s",
             object_name.data(),
             bucket_name.data(),
             response.Error().String().c_str());
        return {};
    }

    return response.etag;
}

bool
common::S3Connection::delete_object(std::string_view bucket_name,
                                    std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    TRACE("Deleting object %s from bucket %s",
          object_name.data(),
          bucket_name.data());
    minio::s3::RemoveObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->RemoveObject(args);
    if (!response) {
        LOGE("Failed to delete object %s from bucket %s: %s",
             object_name.data(),
             bucket_name.data(),
             response.Error().String().c_str());
        return false;
    }

    return true;
}

std::string
common::S3Connection::create_multipart_object(std::string_view bucket_name,
                                              std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    TRACE("Creating multipart object %s in bucket %s",
          object_name.data(),
          bucket_name.data());
    minio::s3::CreateMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->CreateMultipartUpload(args);
    if (!response) {
        LOGE("Failed to create multipart object %s in bucket %s: %s",
             object_name.data(),
             bucket_name.data(),
             response.Error().String().c_str());
        return {};
    }

    return response.upload_id;
}

std::string
common::S3Connection::upload_multipart_object_part(std::string_view bucket_name,
                                                   std::string_view object_name,
                                                   std::string_view upload_id,
                                                   std::span<uint8_t> data,
                                                   unsigned int part_number)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Number of bytes must be positive.");
    EXPECT(part_number, "Part number must be positive.");

    TRACE("Uploading multipart object part %zu for object %s in bucket %s",
          part_number,
          object_name.data(),
          bucket_name.data());

    std::string_view sv(reinterpret_cast<const char*>(data.data()),
                        data.size());

    minio::s3::UploadPartArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.part_number = part_number;
    args.upload_id = upload_id;
    args.data = sv;

    auto response = client_->UploadPart(args);
    if (!response) {
        LOGE("Failed to upload part %zu for object %s in bucket %s: %s",
             part_number,
             object_name.data(),
             bucket_name.data(),
             response.Error().String().c_str());
        return {};
    }

    return response.etag;
}

bool
common::S3Connection::complete_multipart_object(
  std::string_view bucket_name,
  std::string_view object_name,
  std::string_view upload_id,
  const std::list<minio::s3::Part>& parts)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!upload_id.empty(), "Upload id must not be empty.");
    EXPECT(!parts.empty(), "Parts list must not be empty.");

    TRACE("Completing multipart object %s in bucket %s",
          object_name.data(),
          bucket_name.data());
    minio::s3::CompleteMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.upload_id = upload_id;
    args.parts = parts;

    auto response = client_->CompleteMultipartUpload(args);
    if (!response) {
        LOGE("Failed to complete multipart object %s in bucket %s: %s",
             object_name.data(),
             bucket_name.data(),
             response.Error().String().c_str());
        return false;
    }

    return true;
}

common::S3ConnectionPool::S3ConnectionPool(size_t n_connections,
                                           const std::string& endpoint,
                                           const std::string& access_key_id,
                                           const std::string& secret_access_key)
  : is_accepting_connections_{ true }
{
    CHECK(n_connections > 0);

    for (auto i = 0; i < n_connections; ++i) {
        auto connection = std::make_unique<S3Connection>(
          endpoint, access_key_id, secret_access_key);

        if (connection->check_connection()) {
            connections_.push_back(std::make_unique<S3Connection>(
              endpoint, access_key_id, secret_access_key));
        }
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

    if (!is_accepting_connections_ || connections_.empty()) {
        return nullptr;
    }

    auto conn = std::move(connections_.back());
    connections_.pop_back();
    return conn;
}

void
common::S3ConnectionPool::return_connection(
  std::unique_ptr<S3Connection>&& conn)
{
    std::scoped_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
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

namespace {

bool
make_bucket(minio::s3::Client& client, std::string_view bucket_name)
{
    if (bucket_name.empty()) {
        return false;
    }

    TRACE("Creating bucket %s", bucket_name.data());
    minio::s3::MakeBucketArgs args;
    args.bucket = bucket_name;

    auto response = client.MakeBucket(args);

    return (bool)response;
}

bool
destroy_bucket(minio::s3::Client& client, std::string_view bucket_name)
{
    if (bucket_name.empty()) {
        return false;
    }

    TRACE("Destroying bucket %s", bucket_name.data());
    minio::s3::RemoveBucketArgs args;
    args.bucket = bucket_name;

    auto response = client.RemoveBucket(args);

    return (bool)response;
}
} // namespace

extern "C"
{
    acquire_export int unit_test__s3_connection__put_object()
    {
        if (s3_endpoint.empty() || s3_access_key_id.empty() ||
            s3_secret_access_key.empty()) {
            LOGE("S3 credentials not set.");
            return 1;
        }

        minio::s3::BaseUrl url(s3_endpoint);
        url.https = s3_endpoint.starts_with("https");

        minio::creds::StaticProvider provider(s3_access_key_id,
                                              s3_secret_access_key);
        minio::s3::Client client(url, &provider);

        const std::string bucket_name = "acquire-test-bucket";
        const std::string object_name = "test-object";

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (!conn.bucket_exists(bucket_name)) {
                CHECK(make_bucket(client, bucket_name));
                CHECK(conn.bucket_exists(bucket_name));
            }

            CHECK(conn.delete_object(bucket_name, object_name));
            CHECK(!conn.object_exists(bucket_name, object_name));

            std::vector<uint8_t> data(1024, 0);

            std::string etag =
              conn.put_object(bucket_name, object_name, std::span<uint8_t>(data.data(), data.size()));
            CHECK(!etag.empty());

            CHECK(conn.object_exists(bucket_name, object_name));

            // cleanup
            CHECK(conn.delete_object(bucket_name, object_name));
            CHECK(destroy_bucket(client, bucket_name));

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

        minio::s3::BaseUrl url(s3_endpoint);
        url.https = s3_endpoint.starts_with("https");

        minio::creds::StaticProvider provider(s3_access_key_id,
                                              s3_secret_access_key);
        minio::s3::Client client(url, &provider);

        const std::string bucket_name = "acquire-test-bucket";
        const std::string object_name = "test-object";

        int retval = 0;

        try {
            common::S3Connection conn(
              s3_endpoint, s3_access_key_id, s3_secret_access_key);

            if (!conn.bucket_exists(bucket_name)) {
                CHECK(make_bucket(client, bucket_name));
                CHECK(conn.bucket_exists(bucket_name));
            }

            if (conn.object_exists(bucket_name, object_name)) {
                CHECK(conn.delete_object(bucket_name, object_name));
                CHECK(!conn.object_exists(bucket_name, object_name));
            }

            std::string upload_id =
              conn.create_multipart_object(bucket_name, object_name);
            CHECK(!upload_id.empty());

            std::list<minio::s3::Part> parts;

            // parts need to be at least 5MiB, except the last part
            std::vector<uint8_t> data(5 << 20, 0);
            for (auto i = 0; i < 4; ++i) {
                std::string etag = conn.upload_multipart_object_part(
                  bucket_name,
                  object_name,
                  upload_id,
                  std::span<uint8_t>(data.data(), data.size()),
                  i + 1);
                CHECK(!etag.empty());

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
                std::string etag =
                  conn.upload_multipart_object_part(bucket_name,
                                                    object_name,
                                                    upload_id,
                                                    std::span<uint8_t>(data.data(), data.size()),
                                                    part_number);
                CHECK(!etag.empty());

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
            CHECK(destroy_bucket(client, bucket_name));

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
