#include "s3.sink.hh"

#include <regex>

#include <aws/s3/model/CompleteMultipartUploadRequest.h>

namespace zarr = acquire::sink::zarr;

namespace {

} // namespace

template<>
void
zarr::sink_close<zarr::S3Sink>(acquire::sink::zarr::Sink* sink_)
{
    if (!sink_) {
        return;
    }

    if (auto* sink = dynamic_cast<zarr::S3Sink*>(sink_); !sink) {
        LOGE("Failed to cast Sink to S3Sink");
    } else {
        delete sink;
    }
}

zarr::S3Sink::S3Sink(Config&& config)
  : config_{ std::move(config) }
{
}

zarr::S3Sink::~S3Sink()
{
    close_();
}

bool
zarr::S3Sink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    return false;
}

void
zarr::S3Sink::close_()
{
    Aws::S3::Model::CompleteMultipartUploadRequest complete_mpu_request;
}

zarr::S3SinkCreator::S3SinkCreator(
  std::shared_ptr<common::ThreadPool> thread_pool,
  const std::string& access_key_id,
  const std::string& secret_access_key)
  : thread_pool_{ thread_pool }
  , access_key_id_{ access_key_id }
  , secret_access_key_{ secret_access_key }
{
}

bool
zarr::S3SinkCreator::create_chunk_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<Sink*>& chunk_sinks)
{
    return false;
}

bool
zarr::S3SinkCreator::create_shard_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<Sink*>& shard_sinks)
{
    return false;
}

bool
zarr::S3SinkCreator::create_metadata_sinks(
  const std::vector<std::string>& paths,
  std::vector<Sink*>& metadata_sinks)
{
    if (paths.empty()) {
        return true;
    }

    metadata_sinks.clear();
    for (const auto& path : paths) {
        S3Sink::Config config{
            .uri = path,
            .access_key_id = access_key_id_,
            .secret_access_key = secret_access_key_,
        };

        Sink* sink = new S3Sink(std::move(config));
        metadata_sinks.push_back(sink);
    }
    return metadata_sinks.size() == paths.size();
}