#include "zarrv2.writer.hh"

#include "../zarr.hh"
#include "file.sink.hh"
#include "s3.sink.hh"

#include <cmath>
#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV2Writer::ZarrV2Writer(
  const ArrayConfig& config,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(config, thread_pool)
{
}

bool
zarr::ZarrV2Writer::flush_impl_()
{
    // create chunk sinks
    CHECK(sinks_.empty());

    if (is_s3_uri(data_root_)) {
        std::vector<std::string> uri_parts = common::split_uri(data_root_);
        CHECK(uri_parts.size() > 2); // s3://bucket/key
        std::string endpoint = uri_parts.at(0) + "//" + uri_parts.at(1);
        std::string bucket_name = uri_parts.at(2);

        std::string data_root;
        for (auto i = 3; i < uri_parts.size() - 1; ++i) {
            data_root += uri_parts.at(i) + "/";
        }
        if (uri_parts.size() > 2) {
            data_root += uri_parts.back();
        }

        S3SinkCreator creator{ thread_pool_,
                               endpoint,
                               bucket_name,
                               array_config_.access_key_id,
                               array_config_.secret_access_key };

        size_t min_chunk_size_bytes = 0;
        for (const auto& buf : chunk_buffers_) {
            min_chunk_size_bytes = std::max(min_chunk_size_bytes, buf.size());
        }

        CHECK(creator.create_chunk_sinks(
          data_root, array_config_.dimensions, sinks_, min_chunk_size_bytes));
    } else {
        const std::string data_root =
          (fs::path(data_root_) / std::to_string(append_chunk_index_)).string();

        FileCreator file_creator(thread_pool_);
        if (!file_creator.create_chunk_sinks(
              data_root, array_config_.dimensions, sinks_)) {
            return false;
        }
    }

    CHECK(sinks_.size() == chunk_buffers_.size());

    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);

    }

    // wait for all threads to finish
    latch.wait();

    return true;
}

bool
zarr::ZarrV2Writer::should_rollover_() const
{
    return true;
}
