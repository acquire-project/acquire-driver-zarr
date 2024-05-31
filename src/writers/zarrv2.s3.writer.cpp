#include "zarrv2.s3.writer.hh"
#include "s3.sink.hh"

#include <latch>
#include <queue>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

namespace {
bool
create_chunk_sinks(const std::string& data_root,
                   const std::vector<zarr::Dimension>& dimensions,
                   const std::string& endpoint,
                   const std::string& bucket_name,
                   const std::string& access_key_id,
                   const std::string& secret_access_key,
                   std::shared_ptr<common::ThreadPool>& thread_pool,
                   std::vector<std::unique_ptr<zarr::S3Sink>>& sinks)
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

    std::atomic<bool> all_successful = true;

    const auto n_sinks = paths.size();

    sinks.resize(n_sinks);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_sinks);

    for (auto i = 0; i < n_sinks; ++i) {
        const auto path = paths.front();
        paths.pop();

        std::unique_ptr<zarr::S3Sink>* psink = &sinks[i];
        thread_pool->push_to_job_queue([&endpoint,
                                        &bucket_name,
                                        &access_key_id,
                                        &secret_access_key,
                                        path,
                                        psink,
                                        &latch,
                                        &all_successful](
                                         std::string& err) -> bool {
            bool success = false;

            zarr::S3Sink::Config config{
                .endpoint = endpoint,
                .bucket_name = bucket_name,
                .object_key = path,
                .access_key_id = access_key_id,
                .secret_access_key = secret_access_key,
            };

            try {
                if (all_successful) {
                    *psink = std::make_unique<zarr::S3Sink>(std::move(config));
                }
                success = true;
            } catch (const std::exception& exc) {
                char buf[128];
                snprintf(buf,
                         sizeof(buf),
                         "Failed to create sink '%s': %s.",
                         path.c_str(),
                         exc.what());
                err = buf;
            } catch (...) {
                char buf[128];
                snprintf(buf,
                         sizeof(buf),
                         "Failed to create sink '%s': (unknown).",
                         path.c_str());
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
} // namespace

zarr::ZarrV2S3Writer::ZarrV2S3Writer(
  const WriterConfig& writer_config,
  const S3Config& s3_config,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : S3Writer(writer_config, s3_config, thread_pool)
{
}

bool
zarr::ZarrV2S3Writer::flush_impl_()
{
    try {
        CHECK(sinks_.empty());

        CHECK(create_chunk_sinks(writer_config_.data_root,
                                 writer_config_.dimensions,
                                 endpoint_,
                                 bucket_name_,
                                 access_key_id_,
                                 secret_access_key_,
                                 thread_pool_,
                                 sinks_));
        CHECK(sinks_.size() == chunk_buffers_.size());

        std::latch latch(sinks_.size());
        {
            std::scoped_lock lock(buffers_mutex_);
            for (auto i = 0; i < sinks_.size(); ++i) {
                auto& chunk = chunk_buffers_.at(i);
                thread_pool_->push_to_job_queue(
                  std::move([&sink = sinks_.at(i),
                             data = chunk.data(),
                             size = chunk.size(),
                             &latch](std::string& err) -> bool {
                      bool success = false;
                      try {
                          CHECK(sink->write(data, size));
                          success = true;
                      } catch (const std::exception& exc) {
                          char buf[128];
                          snprintf(buf,
                                   sizeof(buf),
                                   "Failed to write chunk: %s",
                                   exc.what());
                          err = buf;
                      } catch (...) {
                          err = "Unknown error";
                      }

                      latch.count_down();
                      return success;
                  }));
            }
        }
        latch.wait();

        return true;
    } catch (const std::exception& exc) {
        LOGE("Failed to flush: %s", exc.what());
    } catch (...) {
        LOGE("Failed to flush: (unknown)");
    }

    return false;
}

bool
zarr::ZarrV2S3Writer::should_rollover_() const
{
    return true;
}

void
zarr::ZarrV2S3Writer::close_()
{
}
