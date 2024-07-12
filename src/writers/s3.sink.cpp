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
}

zarr::S3Sink::~S3Sink()
{
    close_();
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

    std::unique_ptr<common::S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    bool retval = false;

    try {
        std::string etag;
        CHECK(connection->put_object(
          bucket_name_, object_key_, buf_.data(), buf_size_, etag));

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

    std::unique_ptr<common::S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    std::string upload_id = upload_id_;
    if (upload_id.empty()) {
        CHECK(connection->create_multipart_object(
          bucket_name_, object_key_, upload_id));
    }

    minio::s3::Part part;
    part.number = (unsigned int)parts_.size() + 1;

    CHECK(connection->upload_multipart_object_part(bucket_name_,
                                                   object_key_,
                                                   upload_id,
                                                   buf_.data(),
                                                   buf_size_,
                                                   part.number,
                                                   part.etag));

    parts_.push_back(part);

    // set only when the part is successfully uploaded
    upload_id_ = upload_id;

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

    std::unique_ptr<common::S3Connection> connection;
    if (!(connection = connection_pool_->get_connection())) {
        return false;
    }

    bool retval = false;

    try {
        retval = connection->complete_multipart_object(
          bucket_name_, object_key_, upload_id_, parts_);
    } catch (const std::exception& exc) {
        LOGE("Error: %s", exc.what());
    } catch (...) {
        LOGE("Error: (unknown)");
    }

    connection_pool_->release_connection(std::move(connection));

    return retval;
}

void
zarr::S3Sink::close_()
{
    try {
        // upload_id_ is populated after successfully uploading a part
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
