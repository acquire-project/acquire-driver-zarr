#include "macros.hh"
#include "s3.connection.hh"

#include <miniocpp/utils.h>

#include <list>
#include <sstream>
#include <string_view>

zarr::S3Connection::S3Connection(const std::string& endpoint,
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
zarr::S3Connection::check_connection()
{
    return static_cast<bool>(client_->ListBuckets());
}

bool
zarr::S3Connection::bucket_exists(std::string_view bucket_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");

    minio::s3::BucketExistsArgs args;
    args.bucket = bucket_name;

    auto response = client_->BucketExists(args);
    return response.exist;
}

bool
zarr::S3Connection::object_exists(std::string_view bucket_name,
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
zarr::S3Connection::put_object(std::string_view bucket_name,
                               std::string_view object_name,
                               std::span<uint8_t> data)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Data must not be empty.");

    minio::utils::CharBuffer buffer((char*)const_cast<uint8_t*>(data.data()),
                                    data.size());
    std::basic_istream stream(&buffer);

    LOG_DEBUG(
      "Putting object %s in bucket %s", object_name.data(), bucket_name.data());
    minio::s3::PutObjectArgs args(stream, (long)data.size(), 0);
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->PutObject(args);
    if (!response) {
        LOG_ERROR("Failed to put object %s in bucket %s: %s",
                  object_name.data(),
                  bucket_name.data(),
                  response.Error().String().c_str());
        return {};
    }

    return response.etag;
}

bool
zarr::S3Connection::delete_object(std::string_view bucket_name,
                                  std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG("Deleting object %s from bucket %s",
              object_name.data(),
              bucket_name.data());
    minio::s3::RemoveObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->RemoveObject(args);
    if (!response) {
        LOG_ERROR("Failed to delete object %s from bucket %s: %s",
                  object_name.data(),
                  bucket_name.data(),
                  response.Error().String().c_str());
        return false;
    }

    return true;
}

std::string
zarr::S3Connection::create_multipart_object(std::string_view bucket_name,
                                            std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG("Creating multipart object %s in bucket %s",
              object_name.data(),
              bucket_name.data());
    minio::s3::CreateMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = client_->CreateMultipartUpload(args);
    CHECK(!response.upload_id.empty());

    return response.upload_id;
}

std::string
zarr::S3Connection::upload_multipart_object_part(std::string_view bucket_name,
                                                 std::string_view object_name,
                                                 std::string_view upload_id,
                                                 std::span<uint8_t> data,
                                                 unsigned int part_number)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Number of bytes must be positive.");
    EXPECT(part_number, "Part number must be positive.");

    LOG_DEBUG("Uploading multipart object part %zu for object %s in bucket %s",
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
        LOG_ERROR("Failed to upload part %zu for object %s in bucket %s: %s",
                  part_number,
                  object_name.data(),
                  bucket_name.data(),
                  response.Error().String().c_str());
        return {};
    }

    return response.etag;
}

bool
zarr::S3Connection::complete_multipart_object(
  std::string_view bucket_name,
  std::string_view object_name,
  std::string_view upload_id,
  const std::list<minio::s3::Part>& parts)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!upload_id.empty(), "Upload id must not be empty.");
    EXPECT(!parts.empty(), "Parts list must not be empty.");

    LOG_DEBUG("Completing multipart object %s in bucket %s",
              object_name.data(),
              bucket_name.data());
    minio::s3::CompleteMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.upload_id = upload_id;
    args.parts = parts;

    auto response = client_->CompleteMultipartUpload(args);
    if (!response) {
        LOG_ERROR("Failed to complete multipart object %s in bucket %s: %s",
                  object_name.data(),
                  bucket_name.data(),
                  response.Error().String().c_str());
        return false;
    }

    return true;
}

zarr::S3ConnectionPool::S3ConnectionPool(size_t n_connections,
                                         const std::string& endpoint,
                                         const std::string& access_key_id,
                                         const std::string& secret_access_key)
  : is_accepting_connections_{ true }
{
    for (auto i = 0; i < n_connections; ++i) {
        auto connection = std::make_unique<S3Connection>(
          endpoint, access_key_id, secret_access_key);

        if (connection->check_connection()) {
            connections_.push_back(std::make_unique<S3Connection>(
              endpoint, access_key_id, secret_access_key));
        }
    }

    CHECK(!connections_.empty());
}

zarr::S3ConnectionPool::~S3ConnectionPool() noexcept
{
    is_accepting_connections_ = false;
    cv_.notify_all();
}

std::unique_ptr<zarr::S3Connection>
zarr::S3ConnectionPool::get_connection()
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
zarr::S3ConnectionPool::return_connection(std::unique_ptr<S3Connection>&& conn)
{
    std::scoped_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
}
