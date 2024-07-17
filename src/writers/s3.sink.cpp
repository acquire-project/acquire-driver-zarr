#include "s3.sink.hh"

#include "common/utilities.hh"
#include "logger.h"

#include <miniocpp/client.h>

namespace zarr = acquire::sink::zarr;

zarr::S3Sink::S3Sink(const std::string& bucket_name,
                     const std::string& object_key,
                     std::shared_ptr<common::S3ConnectionPool> connection_pool)
  : bucket_name_{ bucket_name }
  , object_key_{ object_key }
  , connection_pool_{ connection_pool }
  , buf_(5 << 20, 0) // 5 MiB is the minimum multipart upload size
{
    CHECK(!bucket_name_.empty());
    CHECK(!object_key_.empty());
    CHECK(connection_pool_);
}

zarr::S3Sink::~S3Sink()
{
    try {
        // upload_id_ is only populated after successfully uploading a part
        if (upload_id_.empty() && buf_size_ > 0) {
            CHECK(put_object_());
        } else if (!upload_id_.empty()) {
            if (buf_size_ > 0) {
                CHECK(flush_part_());
            }
            CHECK(finalize_multipart_upload_());
        }
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }
}

bool
zarr::S3Sink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    CHECK(buf);
    CHECK(bytes_of_buf);

    while (bytes_of_buf > 0) {
        const auto n = std::min(bytes_of_buf, buf_.size() - buf_size_);
        if (n) {
            std::copy(buf, buf + n, buf_.begin() + buf_size_);
            buf_size_ += n;
            buf += n;
            bytes_of_buf -= n;
        }

        if (buf_size_ == buf_.size()) {
            CHECK(flush_part_());
        }
    }

    return true;
}

bool
zarr::S3Sink::put_object_() noexcept
{
    if (buf_size_ == 0) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = false;
    try {
        std::span<uint8_t> data(buf_.data(), buf_size_);
        std::string etag =
          connection->put_object(bucket_name_, object_key_, data);
        EXPECT(
          !etag.empty(), "Failed to upload object: %s", object_key_.c_str());

        retval = true;
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));
    buf_size_ = 0;

    return retval;
}

bool
zarr::S3Sink::flush_part_() noexcept
{
    if (buf_size_ == 0) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = false;
    try {
        std::string upload_id = upload_id_;
        if (upload_id.empty()) {
            upload_id =
              connection->create_multipart_object(bucket_name_, object_key_);
            EXPECT(!upload_id.empty(),
                   "Failed to create multipart object: %s",
                   object_key_.c_str());
        }

        minio::s3::Part part;
        part.number = static_cast<unsigned int>(parts_.size()) + 1;

        std::span<uint8_t> data(buf_.data(), buf_size_);
        part.etag = connection->upload_multipart_object_part(
          bucket_name_, object_key_, upload_id, data, part.number);
        EXPECT(!part.etag.empty(),
               "Failed to upload part %u of object %s",
               part.number,
               object_key_.c_str());

        // set these only when the part is successfully uploaded
        parts_.push_back(part);
        upload_id_ = upload_id;

        retval = true;
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));
    buf_size_ = 0;

    return retval;
}

bool
zarr::S3Sink::finalize_multipart_upload_() noexcept
{
    if (upload_id_.empty()) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = connection->complete_multipart_object(
      bucket_name_, object_key_, upload_id_, parts_);

    connection_pool_->return_connection(std::move(connection));

    return retval;
}
