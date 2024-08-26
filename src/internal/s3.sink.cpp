#include "s3.sink.hh"
#include "logger.hh"

#include <miniocpp/client.h>

#ifdef min
#undef min
#endif

zarr::S3Sink::S3Sink(std::string_view bucket_name,
                     std::string_view object_key,
                     std::shared_ptr<S3ConnectionPool> connection_pool)
  : bucket_name_{ bucket_name }
  , object_key_{ object_key }
  , connection_pool_{ connection_pool }
{
    EXPECT(!bucket_name_.empty(), "Bucket name must not be empty");
    EXPECT(!object_key_.empty(), "Object key must not be empty");
    EXPECT(connection_pool_, "Null pointer: connection_pool");
}

zarr::S3Sink::~S3Sink()
{
    if (!is_multipart_upload_() && n_bytes_buffered_ > 0) {
        if (!put_object_()) {
            LOG_ERROR("Failed to upload object: %s", object_key_.c_str());
        }
    } else if (is_multipart_upload_()) {
        if (n_bytes_buffered_ > 0 && !flush_part_()) {
            LOG_ERROR("Failed to upload part %zu of object %s",
                      parts_.size() + 1,
                      object_key_.c_str());
        }
        if (!finalize_multipart_upload_()) {
            LOG_ERROR("Failed to finalize multipart upload of object %s",
                      object_key_.c_str());
        }
    }
}

bool
zarr::S3Sink::write(size_t _, const uint8_t* data, size_t bytes_of_data)
{
    EXPECT(data, "Null pointer: data");
    if (bytes_of_data == 0)
        return true;

    while (bytes_of_data > 0) {
        const auto bytes_to_write =
          std::min(bytes_of_data, part_buffer_.size() - n_bytes_buffered_);

        if (bytes_to_write) {
            std::copy_n(
              data, bytes_to_write, part_buffer_.begin() + n_bytes_buffered_);
            n_bytes_buffered_ += bytes_to_write;
            data += bytes_to_write;
            bytes_of_data -= bytes_to_write;
        }

        if (n_bytes_buffered_ == part_buffer_.size() && !flush_part_()) {
            return false;
        }
    }

    return true;
}

bool
zarr::S3Sink::put_object_()
{
    if (n_bytes_buffered_ == 0) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = false;
    try {
        std::string etag =
          connection->put_object(bucket_name_,
                                 object_key_,
                                 { part_buffer_.data(), n_bytes_buffered_ });
        EXPECT(
          !etag.empty(), "Failed to upload object: %s", object_key_.c_str());

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: %s", exc.what());
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));
    n_bytes_buffered_ = 0;

    return retval;
}

bool
zarr::S3Sink::is_multipart_upload_() const
{
    return !upload_id_.empty() && !parts_.empty();
}

std::string
zarr::S3Sink::get_multipart_upload_id_()
{
    if (upload_id_.empty()) {
        upload_id_ =
          connection_pool_->get_connection()->create_multipart_object(
            bucket_name_, object_key_);
    }

    return upload_id_;
}

bool
zarr::S3Sink::flush_part_()
{
    if (n_bytes_buffered_ == 0) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = false;
    try {
        minio::s3::Part part;
        part.number = static_cast<unsigned int>(parts_.size()) + 1;

        std::span<uint8_t> data(part_buffer_.data(), n_bytes_buffered_);
        part.etag =
          connection->upload_multipart_object_part(bucket_name_,
                                                   object_key_,
                                                   get_multipart_upload_id_(),
                                                   data,
                                                   part.number);
        EXPECT(!part.etag.empty(),
               "Failed to upload part %u of object %s",
               part.number,
               object_key_.c_str());

        parts_.push_back(part);

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: %s", exc.what());
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));
    n_bytes_buffered_ = 0;

    return retval;
}

bool
zarr::S3Sink::finalize_multipart_upload_()
{
    if (!is_multipart_upload_()) {
        return false;
    }

    auto connection = connection_pool_->get_connection();

    bool retval = connection->complete_multipart_object(
      bucket_name_, object_key_, upload_id_, parts_);

    connection_pool_->return_connection(std::move(connection));

    return retval;
}
