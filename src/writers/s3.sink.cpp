#include "s3.sink.hh"
#include "../common/utilities.hh"
#include "logger.h"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

namespace zarr = acquire::sink::zarr;

zarr::S3Sink::S3Sink(const std::string& bucket_name,
                     const std::string& object_key,
                     std::shared_ptr<S3ConnectionPool> connection_pool)
  : bucket_name_{ bucket_name }
  , object_key_{ object_key }
  , connection_pool_{ connection_pool }
  , buf_(5 << 20, 0) // 5 MiB is the minimum multipart upload size
{
}

bool
zarr::S3Sink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    CHECK(buf);

    try {
        return write_to_buffer_(buf, bytes_of_buf);
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    return false;
}

void
zarr::S3Sink::close()
{
    try {
        if (upload_id_.empty()) {
            CHECK(put_object_());
        } else {
            CHECK(flush_part_());
            CHECK(finalize_multipart_upload_());
        }
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }
}

bool
zarr::S3Sink::write_to_buffer_(const uint8_t* buf, size_t bytes_of_buf)
{
    while (bytes_of_buf > 0) {
        const auto n = std::min(bytes_of_buf, buf_.size() - buf_size_);
        std::copy(buf, buf + n, buf_.begin() + buf_size_);
        buf_size_ += n;
        buf += n;
        bytes_of_buf -= n;

        if (buf_size_ == buf_.size() && !flush_part_()) {
            return false;
        }
    }

    return true;
}

bool
zarr::S3Sink::put_object_()
{
    if (buf_size_ == 0) {
        return true;
    }

    std::shared_ptr<S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    bool retval = false;

    try {
        std::shared_ptr<Aws::S3::S3Client> client = connection->client();

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name_.c_str());
        request.SetKey(object_key_.c_str());
        request.SetContentType("application/octet-stream");

        auto upload_stream_ptr =
          Aws::MakeShared<Aws::StringStream>(object_key_.c_str());
        const auto* data = reinterpret_cast<const char*>(buf_.data());
        upload_stream_ptr->write(data, (std::streamsize)buf_.size());
        request.SetBody(upload_stream_ptr);

        auto outcome = client->PutObject(request);
        CHECK(outcome.IsSuccess());

        retval = true;
        buf_size_ = 0;
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    // cleanup
    connection_pool_->release_connection(std::move(connection));

    return retval;
}

bool
zarr::S3Sink::flush_part_()
{
    if (buf_size_ == 0) {
        return true;
    }

    std::shared_ptr<S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    std::shared_ptr<Aws::S3::S3Client> client = connection->client();

    // initiate upload request
    if (upload_id_.empty()) {
        Aws::S3::Model::CreateMultipartUploadRequest create_request;
        create_request.SetBucket(bucket_name_.c_str());
        create_request.SetKey(object_key_.c_str());
        create_request.SetContentType("application/octet-stream");

        auto create_outcome = client->CreateMultipartUpload(create_request);
        upload_id_ = create_outcome.GetResult().GetUploadId();
    }

    auto part_number = (int)callables_.size() + 1;

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(bucket_name_.c_str());
    request.SetKey(object_key_.c_str());
    request.SetPartNumber(part_number);
    request.SetUploadId(upload_id_.c_str());

    auto upload_stream_ptr =
      Aws::MakeShared<Aws::StringStream>(object_key_.c_str());

    const auto* data = reinterpret_cast<const char*>(buf_.data());
    upload_stream_ptr->write(data, (std::streamsize)buf_.size());
    request.SetBody(upload_stream_ptr);

    auto part_md5(Aws::Utils::HashingUtils::CalculateMD5(*upload_stream_ptr));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    callables_.push_back(client->UploadPartCallable(request));

    // cleanup
    connection_pool_->release_connection(std::move(connection));
    buf_size_ = 0;

    return true;
}

bool
zarr::S3Sink::finalize_multipart_upload_()
{
    if (upload_id_.empty()) {
        return true;
    }

    std::shared_ptr<S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    std::shared_ptr<Aws::S3::S3Client> client = connection->client();

    bool retval = false;

    try {
        Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
        complete_request.SetBucket(bucket_name_.c_str());
        complete_request.SetKey(object_key_.c_str());
        complete_request.SetUploadId(upload_id_.c_str());

        std::vector<Aws::S3::Model::CompletedPart> parts;
        for (auto i = 0; i < callables_.size(); ++i) {
            auto& callable = callables_.at(i);
            auto part_outcome = callable.get();
            const auto etag = part_outcome.GetResult().GetETag();
            CHECK(!etag.empty());

            Aws::S3::Model::CompletedPart part;
            part.SetPartNumber(i + 1);
            part.SetETag(etag);

            parts.push_back(part);
        }

        Aws::S3::Model::CompletedMultipartUpload completed_mpu;
        for (const auto& part : parts) {
            completed_mpu.AddParts(part);
        }
        complete_request.WithMultipartUpload(completed_mpu);

        auto outcome = client->CompleteMultipartUpload(complete_request);
        CHECK(outcome.IsSuccess());

        // cleanup
        upload_id_.clear();
        callables_.clear();

        retval = true;
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    connection_pool_->release_connection(std::move(connection));

    return retval;
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif // _WIN32

#include <aws/core/Aws.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>

#if __has_include("credentials.hpp")
#include "credentials.hpp"
#endif

void
validate_object_exists(const std::string& bucket_name,
                       const std::string& object_key,
                       const std::shared_ptr<Aws::S3::S3Client>& client)
{
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_key.c_str());

    auto outcome = client->HeadObject(request);
    CHECK(outcome.IsSuccess());
}

void
validate_object_contents(const std::string& bucket_name,
                         const std::string& object_key,
                         const std::shared_ptr<Aws::S3::S3Client>& client,
                         const std::vector<uint8_t>& expected_data)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_key.c_str());

    auto outcome = client->GetObject(request);
    CHECK(outcome.IsSuccess());

    auto& stream = outcome.GetResultWithOwnership().GetBody();
    std::string data;
    data.resize(expected_data.size());
    stream.read(&data[0], data.size());

    for (auto i = 0; i < data.size(); ++i) {
        CHECK(data.at(i) == expected_data.at(i));
    }
}

void
delete_object(const std::string& bucket_name,
              const std::string& object_key,
              const std::shared_ptr<Aws::S3::S3Client>& client)
{
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(object_key.c_str());

    auto outcome = client->DeleteObject(request);
    CHECK(outcome.IsSuccess());
}

extern "C"
{
    acquire_export int unit_test__s3_sink__write_put_object()
    {
#ifdef ZARR_S3_ENDPOINT
        int retval = 0;

        const std::string object_key = "test-put-object";

        Aws::SDKOptions options;
        Aws::InitAPI(options);

        try {
            auto connection_pool = std::make_shared<zarr::S3ConnectionPool>(
              1,
              ZARR_S3_ENDPOINT,
              ZARR_S3_ACCESS_KEY_ID,
              ZARR_S3_SECRET_ACCESS_KEY);
            zarr::S3Sink sink(ZARR_S3_BUCKET_NAME, object_key, connection_pool);

            const std::string data = "Hello, Acquire!";
            CHECK(sink.write(0, (const uint8_t*)data.c_str(), data.size()));

            sink.close();

            auto connection = connection_pool->get_connection();
            CHECK(connection);
            auto client = connection->client();

            // check that the object exists
            validate_object_exists(ZARR_S3_BUCKET_NAME, object_key, client);

            // validate object contents
            std::vector<uint8_t> expected_data;

            // contains "Hello, Acquire!" followed by ~5MB of zeros
            expected_data.insert(expected_data.end(), data.begin(), data.end());
            expected_data.resize(5 << 20, 0);

            validate_object_contents(
              ZARR_S3_BUCKET_NAME, object_key, client, expected_data);

            // cleanup
            delete_object(ZARR_S3_BUCKET_NAME, object_key, client);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Caught exception: %s", exc.what());
        } catch (...) {
            LOGE("Caught unknown exception");
        }

        Aws::ShutdownAPI(options);
        return retval;
#else
        return 1;
#endif
    }

    acquire_export int unit_test__s3_sink__write_multipart()
    {
#ifdef ZARR_S3_ENDPOINT
        int retval = 0;

        const std::string object_key = "test-multipart-object";

        Aws::SDKOptions options;
        Aws::InitAPI(options);

        try {
            auto connection_pool = std::make_shared<zarr::S3ConnectionPool>(
              1,
              ZARR_S3_ENDPOINT,
              ZARR_S3_ACCESS_KEY_ID,
              ZARR_S3_SECRET_ACCESS_KEY);
            zarr::S3Sink sink(ZARR_S3_BUCKET_NAME, object_key, connection_pool);

            const std::string data = "Hello, Acquire!";
            for (auto i = 0; i < 5 << 20; ++i) {

                CHECK(sink.write(0, // offset is ignored for S3 writes
                                 (const uint8_t*)data.c_str(),
                                 data.size()));
            }

            sink.close();

            auto connection = connection_pool->get_connection();
            CHECK(connection);
            auto client = connection->client();

            // check that the object exists
            validate_object_exists(ZARR_S3_BUCKET_NAME, object_key, client);

            // validate object contents

            // contains "Hello, Acquire!" repeated 5MB times
            std::vector<uint8_t> expected_data;
            for (auto i = 0; i < 5 << 20; ++i) {
                expected_data.insert(
                  expected_data.end(), data.begin(), data.end());
            }

            validate_object_contents(
              ZARR_S3_BUCKET_NAME, object_key, client, expected_data);

            // cleanup
            delete_object(ZARR_S3_BUCKET_NAME, object_key, client);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Caught exception: %s", exc.what());
        } catch (...) {
            LOGE("Caught unknown exception");
        }

        Aws::ShutdownAPI(options);
        return retval;
#else
        return 1;
#endif
    }
}

#endif // NO_UNIT_TESTS