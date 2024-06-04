#include "zarrv3.s3.writer.hh"
#include "s3.sink.hh"

#include <latch>
#include <queue>

typedef const std::string string;
namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

namespace {
bool
create_shard_sinks(const std::string& data_root,
                   const std::vector<zarr::Dimension>& dimensions,
                   const std::string& endpoint,
                   const std::string& bucket_name,
                   const std::string& access_key_id,
                   const std::string& secret_access_key,
                   std::shared_ptr<ThreadPool>& thread_pool,
                   std::vector<std::unique_ptr<zarr::S3Sink>>& sinks)
{
    std::queue<std::string> paths;
    paths.push(data_root);

    for (auto i = (int)dimensions.size() - 2; i >= 0; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_shards = common::shards_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_shards; ++k) {
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

zarr::ZarrV3S3Writer::ZarrV3S3Writer(
  const WriterConfig& writer_config,
  const S3Config& s3_config,
  std::shared_ptr<ThreadPool> thread_pool)
  : S3Writer(writer_config, s3_config, thread_pool)
  , shard_tables_{ common::number_of_shards(writer_config.dimensions) }
{
    const auto chunks_per_shard =
      common::chunks_per_shard(writer_config.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }
}

bool
zarr::ZarrV3S3Writer::flush_impl_()
{
    try {
        // create sinks if they don't exist
        string data_root =
          writer_config_.data_root + "/c" + std::to_string(append_chunk_index_);

        if (sinks_.empty()) {
            CHECK(create_shard_sinks(data_root,
                                     writer_config_.dimensions,
                                     endpoint_,
                                     bucket_name_,
                                     access_key_id_,
                                     secret_access_key_,
                                     thread_pool_,
                                     sinks_));
        }

        const auto n_shards =
          common::number_of_shards(writer_config_.dimensions);
        CHECK(sinks_.size() == n_shards);

        // get shard indices for each chunk
        std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
        for (auto i = 0; i < chunk_buffers_.size(); ++i) {
            const auto index =
              common::shard_index_for_chunk(i, writer_config_.dimensions);
            chunk_in_shards.at(index).push_back(i);
        }

        bool write_table = is_finalizing_ || should_rollover_();

        // write chunks to sinks
        std::latch latch(n_shards);
        {

        }
        latch.wait();
    } catch (const std::exception& exc) {
        LOGE("Failed to flush: %s", exc.what());
    } catch (...) {
        LOGE("Failed to flush: (unknown)");
    }

    return false;
}

bool
zarr::ZarrV3S3Writer::should_rollover_() const
{
    return false;
}

void
zarr::ZarrV3S3Writer::close_()
{
}