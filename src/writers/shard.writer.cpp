#include "shard.writer.hh"
#include "../zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ShardWriter::ShardWriter(const ImageDims& frame_dims,
                               const ImageDims& shard_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root,
                               std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root, thread_pool)
  , shard_dims_{ shard_dims }
{
    shards_per_frame_x_ =
      std::ceil((float)frame_dims.cols / (float)shard_dims.cols);
    shards_per_frame_y_ =
      std::ceil((float)frame_dims.rows / (float)shard_dims.rows);
}

zarr::ShardWriter::ShardWriter(const ImageDims& frame_dims,
                               const ImageDims& shard_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root,
                               std::shared_ptr<common::ThreadPool> thread_pool,
                               const BloscCompressionParams& compression_params)
  : Writer(frame_dims,
           tile_dims,
           frames_per_chunk,
           data_root,
           thread_pool,
           compression_params)
  , shard_dims_{ shard_dims }
{
    shards_per_frame_x_ =
      std::ceil((float)frame_dims.cols / (float)shard_dims.cols);
    shards_per_frame_y_ =
      std::ceil((float)frame_dims.rows / (float)shard_dims.rows);
}

uint16_t
// FIXME (aliddell): this is generalizable and doesn't need to be a method
zarr::ShardWriter::chunks_per_shard_() const
{
    const uint16_t chunks_per_shard_x = shard_dims_.cols / tile_dims_.cols;
    const uint16_t chunks_per_shard_y = shard_dims_.rows / tile_dims_.rows;
    return chunks_per_shard_x * chunks_per_shard_y;
}

uint16_t
zarr::ShardWriter::shards_per_frame_() const
{
    return shards_per_frame_x_ * shards_per_frame_y_;
}

void
zarr::ShardWriter::make_buffers_() noexcept
{
    const auto n_chunks = tiles_per_frame_();

    const auto bytes_per_px = bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_per_px;

    const auto bytes_to_reserve =
      bytes_per_tile * frames_per_chunk_ +
      (blosc_compression_params_.has_value() ? BLOSC_MAX_OVERHEAD : 0);

    for (auto i = 0; i < n_chunks; ++i) {
        chunk_buffers_.emplace_back();
        chunk_buffers_.back().reserve(bytes_to_reserve);
    }

    const auto n_shards = shards_per_frame_();

    for (auto i = 0; i < n_shards; ++i) {
        shard_buffers_.emplace_back();
    }
}

void
zarr::ShardWriter::flush_() noexcept
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    const auto bytes_per_px = bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_per_px;

    if (bytes_to_flush_ % bytes_per_tile != 0) {
        LOGE("Expected bytes to flush to be a multiple of the "
             "number of bytes per tile.");
    }

    // create shard files if necessary
    if (files_.empty() && !make_files_()) {
        return;
    }

    const auto chunks_per_shard = chunks_per_shard_();

    // compress buffers
    compress_buffers_();
    const size_t bytes_of_index = 2 * chunks_per_shard * sizeof(uint64_t);

    const auto max_bytes_per_chunk =
      bytes_per_tile * frames_per_chunk_ +
      (blosc_compression_params_.has_value() ? BLOSC_MAX_OVERHEAD : 0);

    const auto max_bytes_per_shard =
      max_bytes_per_chunk * chunks_per_shard // data
      + bytes_of_index;                      // indices

    // FIXME (aliddell): put this into a job
    // concatenate chunks into shards
    for (auto i = 0; i < shard_buffers_.size(); ++i) {
        auto& shard = shard_buffers_.at(i);
        size_t chunk_index = 0;
        std::vector<uint64_t> chunk_indices;

        shard.reserve(max_bytes_per_shard);

        for (auto j = 0; j < chunks_per_shard; ++j) {
            chunk_indices.push_back(chunk_index); // chunk offset
            const auto k = i * chunks_per_shard + j;

            auto& chunk = chunk_buffers_.at(k);
            chunk_index += chunk.size();
            chunk_indices.push_back(chunk.size()); // chunk extent

            std::copy(chunk.begin(), chunk.end(), std::back_inserter(shard));
            chunk.clear();
            shard.shrink_to_fit();
        }

        // write the indices out at the end of the shard
        const auto* indices =
          reinterpret_cast<const uint8_t*>(chunk_indices.data());
        std::copy(indices,
                  indices + chunk_indices.size() * sizeof(uint64_t),
                  std::back_inserter(shard));
    }

    // write out
    std::latch latch(shard_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < files_.size(); ++i) {
            const auto& shard = shard_buffers_.at(i);
            thread_pool_->push_to_job_queue(
              std::move([fh = &files_.at(i),
                         shard = shard.data(),
                         size = shard.size(),
                         &latch](std::string& err) -> bool {
                  bool success = false;
                  try {
                      success = file_write(fh, 0, shard, shard + size);
                  } catch (const std::exception& exc) {
                      char buf[128];
                      snprintf(buf,
                               sizeof(buf),
                               "Failed to write shard: %s",
                               exc.what());
                      err = buf;
                  } catch (...) {
                      err = "Failed to write shard (unknown)";
                  }
                  latch.count_down();

                  return success;
              }));
        }
    }

    // wait for all threads to finish
    latch.wait();

    // reset buffers
    for (auto& buf : chunk_buffers_) {
        buf.reserve(max_bytes_per_chunk);
    }

    for (auto& buf : shard_buffers_) {
        buf.clear();
        buf.reserve(max_bytes_per_shard);
    }
    bytes_to_flush_ = 0;
}

bool
zarr::ShardWriter::make_files_() noexcept
{
    return file_creator_.create(data_root_ /
                                  ("c" + std::to_string(current_chunk_)),
                                1,
                                shards_per_frame_y_,
                                shards_per_frame_x_,
                                files_);
}
