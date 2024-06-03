#include "zarrv3.writer.hh"

#include "../zarr.hh"
#include "file.sink.hh"
#include "s3.sink.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
bool
is_s3_uri(const std::string& uri)
{
    return uri.starts_with("s3://") || uri.starts_with("http://") ||
           uri.starts_with("https://");
}
} // namespace

zarr::ZarrV3Writer::ZarrV3Writer(
  const ArrayConfig& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
  , shard_file_offsets_(common::number_of_shards(array_spec.dimensions), 0)
  , shard_tables_{ common::number_of_shards(array_spec.dimensions) }
{
    const auto chunks_per_shard =
      common::chunks_per_shard(array_spec.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }
}

bool
zarr::ZarrV3Writer::flush_impl_()
{
    // create shard sinks if they don't exist
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

        CHECK(creator.create_shard_sinks(
          data_root, array_config_.dimensions, sinks_, min_chunk_size_bytes));
    } else {
        const std::string data_root =
          (fs::path(data_root_) / ("c" + std::to_string(append_chunk_index_)))
            .string();

        FileCreator file_creator(thread_pool_);
        if (sinks_.empty() && !file_creator.create_shard_sinks(
                                data_root, array_config_.dimensions, sinks_)) {
            return false;
        }
    }

    const auto n_shards = common::number_of_shards(array_config_.dimensions);
    CHECK(sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index =
          common::shard_index_for_chunk(i, array_config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    // write out chunks to shards
    bool write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        size_t* file_offset = &shard_file_offsets_.at(i);

        thread_pool_->push_to_job_queue([sink = sinks_.at(i),
                                         &chunks,
                                         &chunk_table,
                                         file_offset,
                                         write_table,
                                         &latch,
                                         this](std::string& err) mutable {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    auto& chunk = chunk_buffers_.at(chunk_idx);

                    success =
                      sink->write(*file_offset, chunk.data(), chunk.size());
                    if (!success) {
                        break;
                    }

                    const auto internal_idx = common::shard_internal_index(
                      chunk_idx, array_config_.dimensions);
                    chunk_table.at(2 * internal_idx) = *file_offset;
                    chunk_table.at(2 * internal_idx + 1) = chunk.size();

                    *file_offset += chunk.size();
                }

                if (success && write_table) {
                    const auto* table =
                      reinterpret_cast<const uint8_t*>(chunk_table.data());
                    success =
                      sink->write(*file_offset,
                                  table,
                                  chunk_table.size() * sizeof(uint64_t));
                }
            } catch (const std::exception& exc) {
                char buf[128];
                snprintf(
                  buf, sizeof(buf), "Failed to write chunk: %s", exc.what());
                err = buf;
            } catch (...) {
                err = "Unknown error";
            }

            latch.count_down();
            return success;
        });
    }

    // wait for all threads to finish
    latch.wait();

    // reset shard tables and file offsets
    if (write_table) {
        for (auto& table : shard_tables_) {
            std::fill_n(table.begin(),
                        table.size(),
                        std::numeric_limits<uint64_t>::max());
        }

        std::fill_n(shard_file_offsets_.begin(), shard_file_offsets_.size(), 0);
    }

    return true;
}

bool
zarr::ZarrV3Writer::should_rollover_() const
{
    const auto& dims = array_config_.dimensions;
    size_t frames_before_flush =
      dims.back().chunk_size_px * dims.back().shard_size_chunks;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}
