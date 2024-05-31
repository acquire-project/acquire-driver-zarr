#include "s3.writer.hh"

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

zarr::S3Writer::S3Writer(const std::string& data_root,
                         const S3Config& s3_config)
  : access_key_id_{ s3_config.access_key_id }
  , secret_access_key_{ s3_config.secret_access_key }
{
    auto uri_parts = common::split_uri(data_root);
    CHECK(uri_parts.size() > 2);
    endpoint_ = uri_parts.at(0) + "//" + uri_parts.at(1);

    bucket_name_ = uri_parts.at(2);
    for (size_t i = 3; i < uri_parts.size(); ++i) {
        bucket_name_ += "/" + uri_parts.at(i);
    }
}

void
zarr::S3Writer::close_()
{
    for (auto& sink : sinks_) {
        ;
    }
    sinks_.clear();
}