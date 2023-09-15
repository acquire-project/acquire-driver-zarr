#include "chunk.writer.hh"

#include "../common.hh"

#include <cmath>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
void
worker_thread(zarr::ChonkWriter::ThreadContext* ctx)
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
} // namespace

zarr::ChonkWriter::ChonkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : chunking_encoder_{ frame_dims, tile_dims }
  , frame_dims_{ frame_dims }
  , tile_dims_{ tile_dims }
  , data_root_{ data_root }
  , frames_per_chunk_{ frames_per_chunk }
  , frames_written_{ 0 }
  , threads_(std::thread::hardware_concurrency())
{
    CHECK(tile_dims_.cols > 0);
    CHECK(tile_dims_.rows > 0);
    EXPECT(tile_dims_ <= frame_dims_,
           "Expected tile dimensions to be less than or equal to frame "
           "dimensions.");

    tile_rows_ = std::ceil((float)frame_dims.rows / (float)tile_dims.rows);
    tile_cols_ = std::ceil((float)frame_dims.cols / (float)tile_dims.cols);

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
        ctx.writer = this;
        ctx.should_stop = false;
        ctx.thread = std::thread(worker_thread, &ctx);
    }
}

zarr::ChonkWriter::~ChonkWriter()
{
    for (auto& ctx : threads_) {
        ctx.should_stop = true;
        ctx.cv.notify_one();
        ctx.thread.join();
    }

    close_files_();
}

bool
zarr::ChonkWriter::write(const VideoFrame* frame)
{
    using namespace std::chrono_literals;

    try {
        CHECK(frame);

        // validate the incoming frame shape against the stored frame dims
        EXPECT(frame_dims_.cols == frame->shape.dims.width,
               "Expected frame to have %d columns. Got %d.",
               frame_dims_.cols,
               frame->shape.dims.width);
        EXPECT(frame_dims_.rows == frame->shape.dims.height,
               "Expected frame to have %d rows. Got %d.",
               frame_dims_.rows,
               frame->shape.dims.height);

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
          chunking_encoder_.encode(buf_.data(),
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

std::optional<zarr::ChonkWriter::JobContext>
zarr::ChonkWriter::pop_from_job_queue() noexcept
{
    std::scoped_lock lock(mutex_);
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto job = jobs_.front();
    jobs_.pop();
    return job;
}

uint32_t
zarr::ChonkWriter::frames_written() const noexcept
{
    return frames_written_;
}

void
zarr::ChonkWriter::make_files_()
{
    const auto t = frames_written_ / frames_per_chunk_;
    for (auto y = 0; y < tile_rows_; ++y) {
        for (auto x = 0; x < tile_cols_; ++x) {
            const auto filename = data_root_ / std::to_string(t) /
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
zarr::ChonkWriter::close_files_()
{
    for (auto& file : files_) {
        file_close(&file);
    }
    files_.clear();
}

void
zarr::ChonkWriter::rollover_()
{
    TRACE("Rolling over");

    close_files_();
    make_files_();
}

#ifndef NO_UNIT_TESTS

#include <fstream>

#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

extern "C" acquire_export int
unit_test__chunk_writer_write()
{
    try {
        fs::path data_dir = "data";
        zarr::ImageDims frame_dims{ 16, 16 };
        zarr::ImageDims tile_dims{ 8, 8 };
        zarr::ChonkWriter writer{ frame_dims, tile_dims, 8, data_dir.string() };

        std::vector<uint16_t> frame_data(16 * 16);
        for (auto i = 0; i < 16 * 16; ++i) {
            frame_data[i] = i;
        }

        VideoFrame frame{ 0 };
        frame.bytes_of_frame = sizeof(VideoFrame) + frame_data.size() * 2;
        frame.shape = {
            .dims = { .channels = 1, .width = 16, .height = 16, .planes = 1 },
            .strides = { .channels = 1,
                         .width = 1,
                         .height = 16,
                         .planes = 16 * 16 },
            .type = SampleType_i16,
        };

        auto frame_ptr = (uint8_t*)malloc(frame.bytes_of_frame);
        memcpy(frame_ptr, &frame, sizeof(VideoFrame));
        memcpy(frame_ptr + sizeof(VideoFrame),
               frame_data.data(),
               frame.bytes_of_frame - sizeof(VideoFrame));

        CHECK(writer.write((VideoFrame*)frame_ptr));
        delete frame_ptr;

        // test file contents
        std::vector<uint16_t> buf(8 * 8);

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "0"));
        std::ifstream fh(data_dir / "0" / "0" / "0", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            CHECK(buf.at(i) == i);
            CHECK(buf.at(i + 8) == i + 16);
            CHECK(buf.at(i + 16) == i + 32);
            CHECK(buf.at(i + 24) == i + 48);
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "1"));
        fh = std::ifstream(data_dir / "0" / "0" / "1", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            CHECK(buf.at(i) == i + 8);
            CHECK(buf.at(i + 8) == i + 24);
            CHECK(buf.at(i + 16) == i + 40);
            CHECK(buf.at(i + 24) == i + 56);
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "1" / "0"));
        fh = std::ifstream(data_dir / "0" / "1" / "0", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            CHECK(buf.at(i) == i + 128);
            CHECK(buf.at(i + 8) == i + 144);
            CHECK(buf.at(i + 16) == i + 160);
            CHECK(buf.at(i + 24) == i + 176);
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "1" / "1"));
        fh = std::ifstream(data_dir / "0" / "1" / "1", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            CHECK(buf.at(i) == i + 136);
            CHECK(buf.at(i + 8) == i + 152);
            CHECK(buf.at(i + 16) == i + 168);
            CHECK(buf.at(i + 24) == i + 184);
        }

        fh.close();

        // cleanup
        std::error_code ec;
        fs::remove_all(data_dir, ec);
        if (ec) {
            LOGE("Failed to remove data directory: %s", ec.message().c_str());
        }

        return 1;
    } catch (const std::exception& e) {
        LOGE("Error: %s", e.what());
    } catch (...) {
        LOGE("Unknown error");
    }

    return 0;
}
#endif
