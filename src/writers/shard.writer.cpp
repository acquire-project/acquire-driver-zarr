#include "shard.writer.hh"

#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ShardWriter::ShardWriter(const ImageDims& frame_dims,
                               const ImageDims& shard_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root)
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
                               const BloscCompressionParams& compression_params)
  : Writer(frame_dims,
           tile_dims,
           frames_per_chunk,
           data_root,
           compression_params)
  , shard_dims_{ shard_dims }
{
    shards_per_frame_x_ =
      std::ceil((float)frame_dims.cols / (float)shard_dims.cols);
    shards_per_frame_y_ =
      std::ceil((float)frame_dims.rows / (float)shard_dims.rows);
}

bool
zarr::ShardWriter::write(const VideoFrame* frame) noexcept
{
    using namespace std::chrono_literals;

    if (!validate_frame_(frame)) {
        // log is written in validate_frame
        return false;
    }

    try {
        if (buffers_.empty()) {
            make_buffers_();
        }

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
        LOGE("Failed to write frame: %s", exc.what());
    } catch (...) {
        LOGE("Failed to write frame (unknown)");
    }

    return false;
}

void
zarr::ShardWriter::make_buffers_() noexcept
{
    buffers_.resize(tiles_per_frame_x_ * tiles_per_frame_y_);

    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;

    for (auto& buf : buffers_) {
        buf.resize(frames_per_chunk_ * bytes_per_tile);
        std::fill(buf.begin(), buf.end(), 0);
    }
}

size_t
zarr::ShardWriter::write_bytes_(const uint8_t* buf, size_t buf_size) noexcept
{
    //    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    //    const auto bytes_per_shard =
    //      shard_dims_.cols * shard_dims_.rows * bytes_of_type;
    //    const auto frames_this_chunk = frames_written_ % frames_per_chunk_;
    //
    //    size_t bytes_written = 0;
    //
    //    // write shards to respective buffers
    //    for (auto i = 0; i < shards_per_frame_y_; ++i) {
    //        for (auto j = 0; j < shards_per_frame_x_; ++j) {
    //            size_t offset = bytes_per_shard * frames_this_chunk;
    //
    //            uint8_t* bytes_out =
    //              buffers_.at(i * shards_per_frame_x_ + j).data();
    //            for (auto k = 0; k < shard_dims_.rows; ++k) {
    //                const auto frame_row = i * shard_dims_.rows + k;
    //                if (frame_row < frame_dims_.rows) {
    //                    const auto frame_col = j * shard_dims_.cols;
    //
    //                    const auto buf_offset =
    //                      bytes_of_type *
    //                      (frame_row * frame_dims_.cols + frame_col);
    //
    //                    const auto region_width =
    //                      std::min(frame_col + shard_dims_.cols,
    //                      frame_dims_.cols) - frame_col;
    //
    //                    const auto nbytes = region_width * bytes_of_type;
    //                    memcpy(bytes_out + offset, buf + buf_offset, nbytes);
    //                }
    //                offset += shard_dims_.cols * bytes_of_type;
    //            }
    //            bytes_written += bytes_per_shard;
    //        }
    //    }
    //
    //    return bytes_written;
    // TODO (aliddell): write out tiles to buffers
    return 0;
}

void
zarr::ShardWriter::compress_buffers_() noexcept
{
}

void
zarr::ShardWriter::flush_() noexcept
{
    //    if (bytes_to_flush_ == 0) {
    //        return;
    //    }
    //
    //    using namespace std::chrono_literals;
    //    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    //    const auto bytes_per_shard_tile =
    //      shard_dims_.cols * shard_dims_.rows * bytes_of_type;
    //    if (bytes_to_flush_ % bytes_per_shard_tile != 0) {
    //        LOGE("Expected bytes to flush to be a multiple of the "
    //             "number of bytes per shard.");
    //    }
    //    const auto bytes_per_shard =
    //      bytes_to_flush_ / (shards_per_frame_x_ * shards_per_frame_y_);
    //
    //    // create shard files if necessary
    //    if (files_.empty()) {
    //        make_files_();
    //    }
    //
    //    // compress buffers and write out
    //    compress_buffers_();
    //    {
    //        for (auto i = 0; i < files_.size(); ++i) {
    //            auto& buf = buffers_.at(i);
    //            jobs_.emplace(buf.data(),
    //                          std::min(bytes_per_shard, buf.size()),
    //                          &files_.at(i),
    //                          0);
    //        }
    //    }
    //
    //    // wait for all writers to finish
    //    while (!jobs_.empty()) {
    //        std::this_thread::sleep_for(2ms);
    //    }
    //
    //    for (auto& buf : buffers_) {
    //        if (buf.size() < bytes_per_shard) {
    //            buf.resize(bytes_per_shard);
    //        }
    //        std::fill(buf.begin(), buf.end(), 0);
    //    }
    //    bytes_to_flush_ = 0;
    // TODO (aliddell): concatenate chunks into shards
}

void
zarr::ShardWriter::make_files_()
{
    for (auto y = 0; y < shards_per_frame_y_; ++y) {
        for (auto x = 0; x < shards_per_frame_x_; ++x) {
            const auto filename = data_root_ /
                                  ("c" + std::to_string(current_chunk_)) / "0" /
                                  std::to_string(y) / std::to_string(x);
            fs::create_directories(filename.parent_path());
            files_.emplace_back();
            CHECK(file_create(&files_.back(),
                              filename.string().c_str(),
                              filename.string().size()));
        }
    }
}