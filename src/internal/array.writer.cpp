#include "array.writer.hh"
#include "zarr.common.hh"
#include "logger.hh"

#include <cmath>
#include <functional>
#include <latch>
#include <stdexcept>

#ifdef min
#undef min
#endif

bool
zarr::downsample(const ArrayWriterConfig& config,
                 ArrayWriterConfig& downsampled_config)
{
    // downsample dimensions
    downsampled_config.dimensions.clear();
    for (const auto& dim : config.dimensions) {
        if (dim.kind ==
            ZarrDimensionType_Channel) { // don't downsample channels
            downsampled_config.dimensions.push_back(dim);
        } else {
            const uint32_t array_size_px =
              (dim.array_size_px + (dim.array_size_px % 2)) / 2;

            const uint32_t chunk_size_px =
              dim.array_size_px == 0
                ? dim.chunk_size_px
                : std::min(dim.chunk_size_px, array_size_px);

            CHECK(chunk_size_px);
            const uint32_t n_chunks =
              (array_size_px + chunk_size_px - 1) / chunk_size_px;

            const uint32_t shard_size_chunks =
              dim.array_size_px == 0
                ? 1
                : std::min(n_chunks, dim.shard_size_chunks);

            downsampled_config.dimensions.emplace_back(dim.name,
                                                       dim.kind,
                                                       array_size_px,
                                                       chunk_size_px,
                                                       shard_size_chunks);
        }
    }

    downsampled_config.level_of_detail = config.level_of_detail + 1;
    downsampled_config.bucket_name = config.bucket_name;
    downsampled_config.store_path = config.store_path;

    // copy the Blosc compression parameters
    downsampled_config.compression_params = config.compression_params;

    // can we downsample downsampled_config?
    for (auto i = 0; i < config.dimensions.size(); ++i) {
        // downsampling made the chunk size strictly smaller
        const auto& dim = config.dimensions[i];
        const auto& downsampled_dim = downsampled_config.dimensions[i];

        if (dim.chunk_size_px > downsampled_dim.chunk_size_px) {
            return false;
        }
    }

    return true;
}

/// Writer
zarr::ArrayWriter::ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_{ config }
  , thread_pool_{ thread_pool }
  , s3_connection_pool_{ s3_connection_pool }
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , is_finalizing_{ false }
{
}

size_t
zarr::ArrayWriter::write_frame(const uint8_t* data, size_t nbytes)
{
    if (bytes_of_frame(config_.dimensions, config_.dtype) != nbytes) {
        return 0;
    }

    if (chunk_buffers_.empty()) {
        make_buffers_();
    }

    // split the incoming frame into tiles and write_frame them to the chunk
    // buffers
    const auto bytes_written = write_frame_to_chunks_(data, nbytes);
    bytes_to_flush_ += bytes_written;
    ++frames_written_;

    if (should_flush_()) {
        flush_();
    }

    return bytes_written;
}

void
zarr::ArrayWriter::finalize()
{
    is_finalizing_ = true;
    flush_();
    close_sinks_();
    is_finalizing_ = false;
}

void
zarr::ArrayWriter::make_buffers_() noexcept
{
    const size_t n_chunks = number_of_chunks_in_memory(config_.dimensions);
    chunk_buffers_.resize(n_chunks); // no-op if already the correct size

    const auto nbytes = bytes_per_chunk(config_.dimensions, config_.dtype);

    for (auto& buf : chunk_buffers_) {
        buf.resize(nbytes);
        std::fill_n(buf.begin(), nbytes, 0);
    }
}

size_t
zarr::ArrayWriter::write_frame_to_chunks_(const uint8_t* buf, size_t buf_size)
{
    // break the frame into tiles and write_frame them to the chunk buffers
    const auto bytes_per_px = bytes_of_type(config_.dtype);

    const auto& dimensions = config_.dimensions;

    const auto& x_dim = dimensions.back();
    const auto frame_cols = x_dim.array_size_px;
    const auto tile_cols = x_dim.chunk_size_px;

    const auto& y_dim = dimensions[dimensions.size() - 2];
    const auto frame_rows = y_dim.array_size_px;
    const auto tile_rows = y_dim.chunk_size_px;

    if (tile_cols == 0 || tile_rows == 0) {
        return 0;
    }

    const auto bytes_per_row = tile_cols * bytes_per_px;

    size_t bytes_written = 0;

    const auto n_tiles_x = (frame_cols + tile_cols - 1) / tile_cols;
    const auto n_tiles_y = (frame_rows + tile_rows - 1) / tile_rows;

    // don't take the frame id from the incoming frame, as the camera may have
    // dropped frames
    const auto frame_id = frames_written_;

    // offset among the chunks in the lattice
    const auto group_offset = tile_group_offset(frame_id, dimensions);
    // offset within the chunk
    const auto chunk_offset =
      chunk_internal_offset(frame_id, dimensions, config_.dtype);

    for (auto i = 0; i < n_tiles_y; ++i) {
        // TODO (aliddell): we can optimize this when tiles_per_frame_x_ is 1
        for (auto j = 0; j < n_tiles_x; ++j) {
            const auto c = group_offset + i * n_tiles_x + j;
            auto& chunk = chunk_buffers_[c];
            auto chunk_it =
              chunk.begin() + static_cast<long long>(chunk_offset);

            for (auto k = 0; k < tile_rows; ++k) {
                const auto frame_row = i * tile_rows + k;
                if (frame_row < frame_rows) {
                    const auto frame_col = j * tile_cols;

                    const auto region_width =
                      std::min(frame_col + tile_cols, frame_cols) - frame_col;

                    const auto region_start =
                      bytes_per_px * (frame_row * frame_cols + frame_col);
                    const auto nbytes = region_width * bytes_per_px;
                    const auto region_stop = region_start + nbytes;
                    if (region_stop > buf_size) {
                        LOG_ERROR("Buffer overflow");
                        return bytes_written;
                    }

                    // copy region
                    if (nbytes > std::distance(chunk_it, chunk.end())) {
                        LOG_ERROR("Buffer overflow");
                        return bytes_written;
                    }
                    std::copy(buf + region_start, buf + region_stop, chunk_it);

                    bytes_written += (region_stop - region_start);
                }
                chunk_it += static_cast<long long>(bytes_per_row);
            }
        }
    }

    return bytes_written;
}

bool
zarr::ArrayWriter::should_flush_() const
{
    const auto& dims = config_.dimensions;
    size_t frames_before_flush = dims.front().chunk_size_px;
    for (auto i = 1; i < dims.size() - 2; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

void
zarr::ArrayWriter::compress_buffers_()
{
    if (!config_.compression_params.has_value()) {
        return;
    }

    LOG_DEBUG("Compressing");

    BloscCompressionParams params = config_.compression_params.value();
    const auto bytes_per_px = bytes_of_type(config_.dtype);

    std::scoped_lock lock(buffers_mutex_);
    std::latch latch(chunk_buffers_.size());
    for (auto& chunk : chunk_buffers_) {
        EXPECT(thread_pool_->push_to_job_queue(
                 [&params, buf = &chunk, bytes_per_px, &latch](
                   std::string& err) -> bool {
                     bool success = false;
                     const size_t bytes_of_chunk = buf->size();

                     try {
                         const auto tmp_size =
                           bytes_of_chunk + BLOSC_MAX_OVERHEAD;
                         std::vector<uint8_t> tmp(tmp_size);
                         const auto nb =
                           blosc_compress_ctx(params.clevel,
                                              params.shuffle,
                                              bytes_per_px,
                                              bytes_of_chunk,
                                              buf->data(),
                                              tmp.data(),
                                              tmp_size,
                                              params.codec_id.c_str(),
                                              0 /* blocksize - 0:automatic */,
                                              1);

                         tmp.resize(nb);
                         buf->swap(tmp);

                         success = true;
                     } catch (const std::exception& exc) {
                         char msg[128];
                         snprintf(msg,
                                  sizeof(msg),
                                  "Failed to compress chunk: %s",
                                  exc.what());
                         err = msg;
                     } catch (...) {
                         err = "Failed to compress chunk (unknown)";
                     }
                     latch.count_down();

                     return success;
                 }),
               "Failed to push to job queue");
    }

    // wait for all threads to finish
    latch.wait();
}

void
zarr::ArrayWriter::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // compress buffers and write_frame out
    compress_buffers_();
    CHECK(flush_impl_());

    const auto should_rollover = should_rollover_();
    if (should_rollover) {
        rollover_();
    }

    if (should_rollover || is_finalizing_) {
        CHECK(write_array_metadata_());
    }

    // reset buffers
    make_buffers_();

    // reset state
    bytes_to_flush_ = 0;
}

void
zarr::ArrayWriter::close_sinks_()
{
    data_sinks_.clear();
}

void
zarr::ArrayWriter::rollover_()
{
    LOG_DEBUG("Rolling over");

    close_sinks_();
    ++append_chunk_index_;
}
