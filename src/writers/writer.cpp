#include <stdexcept>
#include "writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <functional>

namespace zarr = acquire::sink::zarr;

zarr::Writer::Writer(const ImageDims& frame_dims,
                     const ImageDims& tile_dims,
                     uint32_t frames_per_chunk,
                     const std::string& data_root,
                     Zarr* zarr)
  : frame_dims_{ frame_dims }
  , tile_dims_{ tile_dims }
  , data_root_{ data_root }
  , frames_per_chunk_{ frames_per_chunk }
  , frames_written_{ 0 }
  , bytes_to_flush_{ 0 }
  , current_chunk_{ 0 }
  , pixel_type_{ SampleTypeCount }
  , zarr_{ zarr }
{
    CHECK(tile_dims_.cols > 0);
    CHECK(tile_dims_.rows > 0);
    EXPECT(tile_dims_ <= frame_dims_,
           "Expected tile dimensions to be less than or equal to frame "
           "dimensions.");

    tiles_per_frame_y_ =
      std::ceil((float)frame_dims.rows / (float)tile_dims.rows);
    tiles_per_frame_x_ =
      std::ceil((float)frame_dims.cols / (float)tile_dims.cols);

    CHECK(frames_per_chunk_ > 0);
    CHECK(!data_root_.empty());

    if (!fs::is_directory(data_root)) {
        std::error_code ec;
        EXPECT(fs::create_directories(data_root_, ec),
               "Failed to create data root directory: %s",
               ec.message().c_str());
    }
}

zarr::Writer::Writer(const ImageDims& frame_dims,
                     const ImageDims& tile_dims,
                     uint32_t frames_per_chunk,
                     const std::string& data_root,
                     Zarr* zarr,
                     const BloscCompressionParams& compression_params)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root, zarr)
{
    blosc_compression_params_ = compression_params;
}

zarr::Writer::~Writer()
{
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
        char buf[128];
        snprintf(buf, sizeof(buf), "Invalid frame: %s", exc.what());
        zarr_->set_error(buf);
    } catch (...) {
        char buf[32];
        snprintf(buf, sizeof(buf), "Invalid frame (unknown)");
        zarr_->set_error(buf);
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

std::vector<size_t>
zarr::Writer::compress_buffers_() noexcept
{
    const size_t bytes_per_chunk = bytes_to_flush_ / tiles_per_frame_();
    std::vector<size_t> buf_sizes;
    if (!blosc_compression_params_.has_value()) {
        for (auto& buf : chunk_buffers_) {
            buf_sizes.push_back(std::min(bytes_per_chunk, buf.size()));
        }
        return buf_sizes;
    }

    TRACE("Compressing");

    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    std::vector<uint8_t> tmp(bytes_per_chunk + BLOSC_MAX_OVERHEAD);

    std::scoped_lock lock(mutex_);
    for (auto& buf : chunk_buffers_) {
        const auto nbytes =
          blosc_compress_ctx(blosc_compression_params_.value().clevel,
                             blosc_compression_params_.value().shuffle,
                             bytes_of_type,
                             bytes_per_chunk,
                             buf.data(),
                             tmp.data(),
                             bytes_per_chunk + BLOSC_MAX_OVERHEAD,
                             blosc_compression_params_.value().codec_id.c_str(),
                             0 /* blocksize - 0:automatic */,
                             (int)std::thread::hardware_concurrency());

        if (nbytes > buf.size()) {
            buf.resize(nbytes);
        }
        memcpy(buf.data(), tmp.data(), nbytes);
        buf_sizes.push_back(nbytes);
    }

    return buf_sizes;
}

uint32_t
zarr::Writer::tiles_per_frame_() const
{
    return (uint32_t)tiles_per_frame_x_ * (uint32_t)tiles_per_frame_y_;
}

void
zarr::Writer::close_files_()
{
    using namespace std::chrono_literals;
    while (0 < zarr_->jobs_on_queue()) {
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
    ++current_chunk_;
}
