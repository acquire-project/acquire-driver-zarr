#include "shard.writer.hh"

#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
void
worker_thread(zarr::ShardWriter::ThreadContext* ctx)
{
    using namespace std::chrono_literals;

    TRACE("Worker thread starting.");
    CHECK(ctx);

    while (true) {
        std::unique_lock lock(ctx->mutex);
        ctx->cv.wait_for(lock, 1ms, [&] { return ctx->should_stop; });

        if (ctx->should_stop) {
            break;
        }

        if (auto job = ctx->writer->pop_from_job_queue(); job.has_value()) {
            CHECK(file_write(
              job->file, job->offset, job->buf, job->buf + job->buf_size));

            lock.unlock();

            // do work
        } else {
            lock.unlock();
            std::this_thread::sleep_for(1ms);
        }
    }

    TRACE("Worker thread exiting.");
}

zarr::ImageDims
make_shard_dims(const zarr::ImageDims& frame_dims,
                const zarr::ImageDims& tile_dims)
{
    return frame_dims; // TODO (aliddell): do it better
}
} // namespace

zarr::ShardWriter::ShardWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root)
  , shard_dims_{ make_shard_dims(frame_dims, tile_dims) }
  , sharding_encoder_{ frame_dims, shard_dims_, tile_dims }
{
}

bool
zarr::ShardWriter::write(VideoFrame* frame) noexcept
{
    using namespace std::chrono_literals;

    if (!validate_frame(frame)) {
        // log is written in validate_frame
        return false;
    }

    try {
        const auto bytes_of_type = common::bytes_of_type(frame->shape.type);
        const auto bytes_per_tile =
          tile_dims_.cols * tile_dims_.rows * bytes_of_type;
        const auto n_tiles = tile_rows_ * tile_cols_;
        const auto bytes_of_tiled_frame = n_tiles * bytes_per_tile;

        // resize the buffer to fit the chunked frame
        if (buf_.size() < bytes_of_tiled_frame) {
            buf_.resize(bytes_of_tiled_frame);
        }

        // encode the frame into the buffer
        const auto bytes_encoded =
          sharding_encoder_.encode(buf_.data(),
                                   bytes_of_tiled_frame,
                                   frame->data,
                                   frame->bytes_of_frame - sizeof(*frame));
        EXPECT(bytes_encoded == bytes_of_tiled_frame,
               "Expected to encode %d bytes. Got %d.",
               bytes_of_tiled_frame,
               bytes_encoded);

        // create chunk files if necessary
        if (files_.empty()) {
            make_files_();
        }

        // write out each chunk
        {
            std::scoped_lock lock(mutex_);

            for (auto i = 0; i < files_.size(); ++i) {
                jobs_.emplace(buf_.data() + i * bytes_per_tile,
                              bytes_per_tile,
                              &files_.at(i),
                              bytes_of_tiled_frame * frames_written_);
            }
        }

        // wait for all writers to finish
        while (true) {
            std::this_thread::sleep_for(5ms);
            std::scoped_lock lock(mutex_);
            if (jobs_.empty()) {
                break;
            }
        }

        if (++frames_written_ % frames_per_chunk_ == 0) {
            rollover_();
        }

        return true;
    } catch (const std::exception& exc) {
        LOGE("Failed to write frame: %s", exc.what());
    } catch (...) {
        LOGE("Failed to write frame (unknown)");
    }

    return false;
}

void
zarr::ShardWriter::flush() noexcept
{
}