#include "s3.sink.hh"

#include <latch>

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

namespace zarr = acquire::sink::zarr;

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
    Aws::Client::ClientConfiguration client_config;
    client_config.endpointOverride = config_.endpoint;
    const Aws::Auth::AWSCredentials credentials(config_.access_key_id,
                                                config_.secret_access_key);
    s3_client_ =
      std::make_unique<Aws::S3::S3Client>(credentials, nullptr, client_config);
    CHECK(s3_client_);

    // initiate upload request
    Aws::S3::Model::CreateMultipartUploadRequest create_request;
    create_request.SetBucket(config_.bucket_name.c_str());
    create_request.SetKey(config_.object_key.c_str());
    create_request.SetContentType("application/octet-stream");

    auto create_outcome = s3_client_->CreateMultipartUpload(create_request);
    upload_id_ = create_outcome.GetResult().GetUploadId();
}

zarr::S3Sink::~S3Sink()
{
    try {
        close_();
    } catch (const std::exception& e) {
        LOGE("Failed to close S3Sink: {}", e.what());
    } catch (...) {
        LOGE("Failed to close S3Sink");
    }
}

bool
zarr::S3Sink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    auto part_number = (int)completed_parts_.size() + 1;

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(config_.bucket_name.c_str());
    request.SetKey(config_.object_key.c_str());
    request.SetPartNumber(part_number);
    request.SetUploadId(upload_id_.c_str());

    auto upload_stream_ptr = Aws::MakeShared<Aws::StringStream>("ExampleTag");
    upload_stream_ptr->write((const char*)buf, (std::streamsize)bytes_of_buf);
    request.SetBody(upload_stream_ptr);

    auto part_md5(Aws::Utils::HashingUtils::CalculateMD5(*upload_stream_ptr));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    auto outcome = s3_client_->UploadPart(request);
    const std::string etag = outcome.GetResult().GetETag();
    if (etag.empty()) {
        return false;
    }

    Aws::S3::Model::CompletedPart completed_part;
    completed_part.SetPartNumber(part_number);
    completed_part.SetETag(etag);
    completed_parts_.push_back(completed_part);

    return true;
}

void
zarr::S3Sink::close_()
{
    if (completed_parts_.empty()) {
        return;
    }

    Aws::S3::Model::CompleteMultipartUploadRequest complete_mpu_request;
    complete_mpu_request.SetBucket(config_.bucket_name.c_str());
    complete_mpu_request.SetKey(config_.object_key.c_str());
    complete_mpu_request.SetUploadId(upload_id_.c_str());

    Aws::S3::Model::CompletedMultipartUpload completed_mpu;
    for (const auto& part : completed_parts_) {
        completed_mpu.AddParts(part);
    }
    complete_mpu_request.WithMultipartUpload(completed_mpu);

    auto outcome = s3_client_->CompleteMultipartUpload(complete_mpu_request);
    CHECK(outcome.IsSuccess());
}

zarr::S3SinkCreator::S3SinkCreator(
  std::shared_ptr<common::ThreadPool> thread_pool,
  const std::string& endpoint,
  const std::string& bucket_name,
  const std::string& access_key_id,
  const std::string& secret_access_key)
  : endpoint_{ endpoint }
  , bucket_name_{ bucket_name }
  , thread_pool_{ thread_pool }
  , access_key_id_{ access_key_id }
  , secret_access_key_{ secret_access_key }
{
}

bool
zarr::S3SinkCreator::create_chunk_sinks(
  const std::string& data_root,
  const std::vector<Dimension>& dimensions,
  std::vector<Sink*>& chunk_sinks)
{
    std::queue<std::string> paths;
    paths.push(data_root);

    for (auto i = (int)dimensions.size() - 2; i >= 0; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_chunks = common::chunks_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_chunks; ++k) {
                paths.push(path + (path.empty() ? "" : "/") +
                           std::to_string(k));
            }
        }
    }

    return make_s3_objects_(paths, chunk_sinks);
}

bool
zarr::S3SinkCreator::create_shard_sinks(
  const std::string& data_root,
  const std::vector<Dimension>& dimensions,
  std::vector<Sink*>& shard_sinks)
{
    std::queue<std::string> paths;
    paths.push(data_root);

    for (auto i = (int)dimensions.size() - 2; i >= 0; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_chunks = common::shards_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_chunks; ++k) {
                paths.push(path + (path.empty() ? "" : "/") +
                           std::to_string(k));
            }
        }
    }

    return make_s3_objects_(paths, shard_sinks);
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
            .endpoint = endpoint_,
            .bucket_name = bucket_name_,
            .object_key = path,
            .access_key_id = access_key_id_,
            .secret_access_key = secret_access_key_,
        };

        metadata_sinks.push_back(new S3Sink(std::move(config)));
    }
    return metadata_sinks.size() == paths.size();
}

bool
zarr::S3SinkCreator::make_s3_objects_(std::queue<std::string>& paths,
                                      std::vector<Sink*>& sinks)
{
    if (paths.empty()) {
        return true;
    }

    std::atomic<bool> all_successful = true;

    const auto n_sinks = paths.size();
    sinks.resize(n_sinks);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_sinks);

    for (auto i = 0; i < n_sinks; ++i) {
        const auto path = paths.front();
        paths.pop();

        //        S3Sink::Config config{
        //            .endpoint = endpoint_,
        //            .bucket_name = bucket_name_,
        //            .object_key = path,
        //            .access_key_id = access_key_id_,
        //            .secret_access_key = secret_access_key_,
        //        };
        std::string endpoint = endpoint_;
        std::string bucket_name = bucket_name_;
        std::string access_key_id = access_key_id_;
        std::string secret_access_key = secret_access_key_;

        Sink** psink = &sinks[i];
        thread_pool_->push_to_job_queue(
          [endpoint,
           bucket_name,
           path,
           access_key_id,
           secret_access_key,
           psink,
           &latch,
           &all_successful](std::string& err) -> bool {
              bool success = false;

              S3Sink::Config config{
                  .endpoint = endpoint,
                  .bucket_name = bucket_name,
                  .object_key = path,
                  .access_key_id = access_key_id,
                  .secret_access_key = secret_access_key,
              };

              try {
                  if (all_successful) {
                      *psink = new S3Sink(std::move(config));
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create sink '%s': %s.",
                           config.object_key.c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': (unknown).",
                           config.object_key.c_str());
                  err = buf;
              }

              latch.count_down();
              all_successful = all_successful && success;

              return success;
          });
    }

    latch.wait();

    return all_successful;
}