#include "chunk.writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root,
                               Zarr* zarr)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root, zarr)
{
}

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root,
                               Zarr* zarr,
                               const BloscCompressionParams& compression_params)
  : Writer(frame_dims,
           tile_dims,
           frames_per_chunk,
           data_root,
           zarr,
           compression_params)
{
}

bool
zarr::ChunkWriter::write(const VideoFrame* frame) noexcept
{
    using namespace std::chrono_literals;

    if (!validate_frame_(frame)) {
        // log is written in validate_frame
        return false;
    }

    try {
        if (chunk_buffers_.empty()) {
            make_buffers_();
        }

        // write out
        bytes_to_flush_ +=
          write_bytes_(frame->data, frame->bytes_of_frame - sizeof(*frame));

        ++frames_written_;

        // rollover if necessary
        const auto frames_this_chunk = frames_written_ % frames_per_chunk_;
        if (frames_written_ > 0 && frames_this_chunk == 0) {
            flush_();
            rollover_();
        }
        return true;
    } catch (const std::exception& exc) {
        char buf[128];
        snprintf(buf, sizeof(buf), "Failed to write frame: %s", exc.what());
        zarr_->set_error(buf);
    } catch (...) {
        char buf[32];
        snprintf(buf, sizeof(buf), "Failed to write frame (unknown)");
        zarr_->set_error(buf);
    }

    return false;
}

void
zarr::ChunkWriter::make_buffers_() noexcept
{
    const auto nchunks = tiles_per_frame_();
    chunk_buffers_.resize(nchunks);
    buffers_ready_ = new bool[nchunks];
    std::fill(buffers_ready_, buffers_ready_ + nchunks, true);

    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;

    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        auto& buf = chunk_buffers_.at(i);
        buf.resize(frames_per_chunk_ * bytes_per_tile);
        std::fill(buf.begin(), buf.end(), 0);
    }
}

size_t
zarr::ChunkWriter::write_bytes_(const uint8_t* buf, size_t buf_size) noexcept
{
    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;
    const auto frames_this_chunk = frames_written_ % frames_per_chunk_;

    size_t bytes_written = 0;

    for (auto i = 0; i < tiles_per_frame_y_; ++i) {
        for (auto j = 0; j < tiles_per_frame_x_; ++j) {
            size_t offset = bytes_per_tile * frames_this_chunk;

            uint8_t* bytes_out =
              chunk_buffers_.at(i * tiles_per_frame_x_ + j).data();
            for (auto k = 0; k < tile_dims_.rows; ++k) {
                const auto frame_row = i * tile_dims_.rows + k;
                if (frame_row < frame_dims_.rows) {
                    const auto frame_col = j * tile_dims_.cols;

                    const auto buf_offset =
                      bytes_of_type *
                      (frame_row * frame_dims_.cols + frame_col);

                    const auto region_width =
                      std::min(frame_col + tile_dims_.cols, frame_dims_.cols) -
                      frame_col;

                    const auto nbytes = region_width * bytes_of_type;
                    memcpy(bytes_out + offset, buf + buf_offset, nbytes);
                }
                offset += tile_dims_.cols * bytes_of_type;
            }
            bytes_written += bytes_per_tile;
        }
    }

    return bytes_written;
}

void
zarr::ChunkWriter::flush_() noexcept
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    using namespace std::chrono_literals;
    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;
    if (bytes_to_flush_ % bytes_per_tile != 0) {
        LOGE("Expected bytes to flush to be a multiple of the "
             "number of bytes per tile.");
    }

    // create chunk files if necessary
    if (files_.empty() && !make_files_()) {
        zarr_->set_error("Failed to flush.");
        return;
    }

    // compress buffers and write out
    auto buf_sizes = compress_buffers_();
    std::fill(buffers_ready_, buffers_ready_ + chunk_buffers_.size(), false);
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < files_.size(); ++i) {
            auto& buf = chunk_buffers_.at(i);
            zarr_->push_to_job_queue(std::move(
              [fh = &files_.at(i),
               data = buf.data(),
               size = buf_sizes.at(i),
               finished = buffers_ready_ + i](std::string& err) -> bool {
                  bool success = false;
                  try {
                      success = file_write(fh, 0, data, data + size);
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
                  *finished = true;

                  return success;
              }));
        }
    }

    // wait for all threads to finish
    while (!std::all_of(buffers_ready_,
                        buffers_ready_ + chunk_buffers_.size(),
                        [](const auto& b) { return b; })) {
        std::this_thread::sleep_for(500us);
    }

    // reset buffers
    const auto bytes_per_chunk =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type * frames_per_chunk_;
    for (auto& buf : chunk_buffers_) {
        // absurd edge case we need to account for
        if (buf.size() > bytes_per_chunk) {
            buf.resize(bytes_per_chunk);
        }

        std::fill(buf.begin(), buf.end(), 0);
    }
    bytes_to_flush_ = 0;
}

bool
zarr::ChunkWriter::make_files_() noexcept
{
    file_creator_.set_base_dir(data_root_ / std::to_string(current_chunk_));
    return file_creator_.create(
      1, tiles_per_frame_y_, tiles_per_frame_x_, files_);
}
