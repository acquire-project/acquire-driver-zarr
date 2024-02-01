#include "zarrv3.writer.hh"
#include "../zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV3Writer::ZarrV3Writer(
  const ArraySpec& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
{
}

void
zarr::ZarrV3Writer::flush_()
{
    //    if (bytes_to_flush_ == 0) {
    //        return;
    //    }
    //
    //    // create shard files if necessary
    //    if (files_.empty() && !file_creator_.create_files(
    //                            data_root_ / ("c" +
    //                            std::to_string(current_chunk_)),
    //                            array_spec_.dimensions,
    //                            true,
    //                            files_)) {
    //        return;
    //    }
    //
    //    const auto chunks_per_shard = chunks_per_shard_();
    //
    //    // compress buffers
    //    compress_buffers_();
    //    const size_t bytes_of_index = 2 * chunks_per_shard * sizeof(uint64_t);
    //
    //    const auto max_bytes_per_chunk =
    //      bytes_per_tile * frames_per_chunk_ +
    //      (blosc_compression_params_.has_value() ? BLOSC_MAX_OVERHEAD : 0);
    //
    //    // concatenate chunks into shards
    //    const auto n_shards = shards_per_frame_();
    //    std::latch latch(n_shards);
    //    for (auto i = 0; i < n_shards; ++i) {
    //        thread_pool_->push_to_job_queue(
    //          std::move([fh = &files_.at(i), chunks_per_shard, i, &latch,
    //          this](
    //                      std::string& err) {
    //              size_t chunk_index = 0;
    //              std::vector<uint64_t> chunk_indices;
    //              size_t offset = 0;
    //              bool success = false;
    //              try {
    //                  for (auto j = 0; j < chunks_per_shard; ++j) {
    //                      chunk_indices.push_back(chunk_index); // chunk
    //                      offset const auto k = i * chunks_per_shard + j;
    //
    //                      auto& chunk = chunk_buffers_.at(k);
    //                      chunk_index += chunk.size();
    //                      chunk_indices.push_back(chunk.size()); // chunk
    //                      extent
    //
    //                      file_write(
    //                        fh, offset, chunk.data(), chunk.data() +
    //                        chunk.size());
    //                      offset += chunk.size();
    //                  }
    //
    //                  // write the indices out at the end of the shard
    //                  const auto* indices =
    //                    reinterpret_cast<const
    //                    uint8_t*>(chunk_indices.data());
    //                  success = (bool)file_write(fh,
    //                                             offset,
    //                                             indices,
    //                                             indices +
    //                                             chunk_indices.size() *
    //                                                         sizeof(uint64_t));
    //              } catch (const std::exception& exc) {
    //                  char buf[128];
    //                  snprintf(
    //                    buf, sizeof(buf), "Failed to write chunk: %s",
    //                    exc.what());
    //                  err = buf;
    //              } catch (...) {
    //                  err = "Unknown error";
    //              }
    //
    //              latch.count_down();
    //              return success;
    //          }));
    //    }
    //
    //    // wait for all threads to finish
    //    latch.wait();
    //
    //    // reset buffers
    //    for (auto& buf : chunk_buffers_) {
    //        buf.clear();
    //        buf.reserve(max_bytes_per_chunk);
    //    }
    //    bytes_to_flush_ = 0;
}
