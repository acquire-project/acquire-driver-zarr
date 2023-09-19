#include "writer.hh"

#include <cmath>
#include <functional>

namespace zarr = acquire::sink::zarr;

zarr::Writer::Writer(const ImageDims& frame_dims,
                     const ImageDims& tile_dims,
                     uint32_t frames_per_chunk,
                     const std::string& data_root)
  : frame_dims_{ frame_dims }
  , tile_dims_{ tile_dims }
  , data_root_{ data_root }
  , frames_per_chunk_{ frames_per_chunk }
  , frames_written_{ 0 }
  , bytes_to_flush_{ 0 }
  , t_{ 0 }
  , threads_(std::thread::hardware_concurrency())
  , pixel_type_{ SampleTypeCount }
{
    CHECK(tile_dims_.cols > 0);
    CHECK(tile_dims_.rows > 0);
    EXPECT(tile_dims_ <= frame_dims_,
           "Expected tile dimensions to be less than or equal to frame "
           "dimensions.");

    tile_rows_ = std::ceil((float)frame_dims.rows / (float)tile_dims.rows);
    tile_cols_ = std::ceil((float)frame_dims.cols / (float)tile_dims.cols);

    // pare down the number of threads if we have too many
    while (threads_.size() > tile_rows_ * tile_cols_) {
        threads_.pop_back();
    }

    CHECK(frames_per_chunk_ > 0);
    CHECK(!data_root_.empty());

    if (!fs::is_directory(data_root)) {
        std::error_code ec;
        EXPECT(fs::create_directories(data_root_, ec),
               "Failed to create data root directory: %s",
               ec.message().c_str());
    }

    // spin up threads
    for (auto& ctx : threads_) {
        ctx.should_stop = false;
        ctx.thread =
          std::thread([this, capture0 = &ctx] { worker_thread_(capture0); });
    }
}

zarr::Writer::~Writer()
{
    for (auto& ctx : threads_) {
        ctx.should_stop = true;
        ctx.cv.notify_one();
        ctx.thread.join();
    }
}

void
zarr::Writer::finalize() noexcept
{
    using namespace std::chrono_literals;
    finalize_chunks_();
    if (bytes_to_flush_ > 0) {
        flush_();
    }

    close_files_();
}

uint32_t
zarr::Writer::frames_written() const noexcept
{
    return frames_written_;
}

bool
zarr::Writer::validate_frame_(const VideoFrame* frame) noexcept
{
    try {
        CHECK(frame);

        if (pixel_type_ == SampleTypeCount) {
            pixel_type_ = frame->shape.type;
        } else {
            EXPECT(pixel_type_ == frame->shape.type,
                   "Expected frame to have pixel type %s. Got %s.",
                   common::sample_type_to_string(pixel_type_),
                   common::sample_type_to_string(frame->shape.type));
        }

        // validate the incoming frame shape against the stored frame dims
        EXPECT(frame_dims_.cols == frame->shape.dims.width,
               "Expected frame to have %d columns. Got %d.",
               frame_dims_.cols,
               frame->shape.dims.width);
        EXPECT(frame_dims_.rows == frame->shape.dims.height,
               "Expected frame to have %d rows. Got %d.",
               frame_dims_.rows,
               frame->shape.dims.height);

        return true;
    } catch (const std::exception& exc) {
        LOGE("Invalid frame: %s", exc.what());
    } catch (...) {
        LOGE("Invalid frame: (unknown)");
    }
    return false;
}

void
zarr::Writer::finalize_chunks_() noexcept
{
    using namespace std::chrono_literals;

    const auto frames_this_chunk = frames_written_ % frames_per_chunk_;

    // don't write zeros if we have written less than one full chunk or if
    // the last frame written was the final frame in its chunk
    if (frames_written_ < frames_per_chunk_ || frames_this_chunk == 0) {
        return;
    }
    const auto bytes_per_frame =
      frame_dims_.rows * frame_dims_.cols * common::bytes_of_type(pixel_type_);
    const auto frames_to_write = frames_per_chunk_ - frames_this_chunk;

    bytes_to_flush_ += frames_to_write * bytes_per_frame;
}

void
zarr::Writer::make_files_()
{
    for (auto y = 0; y < tile_rows_; ++y) {
        for (auto x = 0; x < tile_cols_; ++x) {
            const auto filename = data_root_ / std::to_string(t_) / "0" /
                                  std::to_string(y) / std::to_string(x);
            fs::create_directories(filename.parent_path());
            files_.emplace_back();
            CHECK(file_create(&files_.back(),
                              filename.string().c_str(),
                              filename.string().size()));
        }
    }
}

void
zarr::Writer::close_files_()
{
    using namespace std::chrono_literals;
    while (!jobs_.empty()) {
        std::this_thread::sleep_for(2ms);
    }

    for (auto& file : files_) {
        file_close(&file);
    }
    files_.clear();
}

void
zarr::Writer::rollover_()
{
    TRACE("Rolling over");

    close_files_();
    ++t_;
}

std::optional<zarr::Writer::JobContext>
zarr::Writer::pop_from_job_queue() noexcept
{
    std::scoped_lock lock(mutex_);
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto job = jobs_.front();
    jobs_.pop();
    return job;
}

void
zarr::Writer::worker_thread_(ThreadContext* ctx)
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

        if (auto job = pop_from_job_queue(); job.has_value()) {
            CHECK(file_write(
              job->fh, job->offset, job->buf, job->buf + job->buf_size));
            lock.unlock();
        } else {
            lock.unlock();
            std::this_thread::sleep_for(1ms);
        }
    }

    TRACE("Worker thread exiting.");
}