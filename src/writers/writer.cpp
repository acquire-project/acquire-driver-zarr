#include <stdexcept>
#include "writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <functional>

namespace zarr = acquire::sink::zarr;

/// DirectoryCreator
zarr::FileCreator::FileCreator(Zarr* zarr)
  : zarr_{ zarr }
{
}

void
zarr::FileCreator::set_base_dir(const fs::path& base_dir) noexcept
{
    base_dir_ = base_dir;
    fs::create_directories(base_dir_);
}

bool
zarr::FileCreator::create(int n_c,
                          int n_y,
                          int n_x,
                          std::vector<file>& files) noexcept
{
    using namespace std::chrono_literals;

    std::vector<std::shared_ptr<std::mutex>> mutexes;
    for (auto i = 0; i < n_c; ++i) {
        mutexes.push_back(std::make_shared<std::mutex>());
    }

    std::vector<int> finished(n_c * n_y, 0);

    if (!create_channel_dirs_(n_c)) {
        return false;
    }

    files.resize(n_c * n_y * n_x);

    // until we support more than one channel, n_c will always be 1
    for (auto c = 0; c < n_c; ++c) {
        for (auto y = 0; y < n_y; ++y) {
            zarr_->push_to_job_queue(
              [base = base_dir_,
               files = files.data() + c * n_y * n_x + y * n_x,
               mtx = mutexes.at(c),
               c,
               y,
               n_x,
               done = finished.data() + c * n_y + y](std::string& err) -> bool {
                  bool success = false;
                  try {
                      auto path = base / std::to_string(c);
                      {
                          std::unique_lock lock(*mtx);
                          while (!fs::exists(path)) {
                              lock.unlock();
                              std::this_thread::sleep_for(1ms);
                              lock.lock();
                          }
                      }

                      path /= std::to_string(y);

                      if (fs::exists(path)) {
                          EXPECT(fs::is_directory(path),
                                 "%s must be a directory.",
                                 path.c_str());
                      } else {
                          EXPECT(fs::create_directories(path),
                                 "Failed to create directory: %s",
                                 path.c_str());
                      }

                      for (auto x = 0; x < n_x; ++x) {
                          auto& file = files[x];
                          auto file_path = path / std::to_string(x);

                          EXPECT(file_create(&file,
                                             file_path.string().c_str(),
                                             file_path.string().size()),
                                 "Failed to open file: '%s'",
                                 file_path.c_str());
                      }

                      success = true;
                  } catch (const std::exception& exc) {
                      char buf[128];
                      snprintf(buf,
                               sizeof(buf),
                               "Failed to create directory: %s",
                               exc.what());
                      err = buf;
                  } catch (...) {
                      err = "Failed to create directory (unknown)";
                  }

                  *done = success ? 1 : -1;
                  return success;
              });
        }
    }

    while (!std::all_of(
      finished.begin(), finished.end(), [](const auto& f) { return f != 0; })) {
        std::this_thread::sleep_for(500us);
    }

    return std::all_of(
      finished.begin(), finished.end(), [](const auto& f) { return f == 1; });
}

bool
zarr::FileCreator::create_channel_dirs_(int n_c) noexcept
{
    using namespace std::chrono_literals;

    std::vector<int> finished(n_c, 0);
    for (auto c = 0; c < n_c; ++c) {
        // create the channel directory
        zarr_->push_to_job_queue(
          [base = base_dir_, c, done = finished.data() + c](
            std::string& err) -> bool {
              bool success = false;
              try {
                  const auto path = base / std::to_string(c);
                  if (fs::exists(path)) {
                      EXPECT(fs::is_directory(path),
                             "%s must be a directory.",
                             path.c_str());
                  } else {
                      EXPECT(fs::create_directories(path),
                             "Failed to create directory: %s",
                             path.c_str());
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory: %s",
                           exc.what());
                  err = buf;
              } catch (...) {
                  err = "Failed to create directory (unknown)";
              }
              *done = success ? 1 : -1;
              return success;
          });
    }

    while (!std::all_of(
      finished.begin(), finished.end(), [](const auto& f) { return f != 0; })) {
        std::this_thread::sleep_for(500us);
    }

    return std::all_of(
      finished.begin(), finished.end(), [](const auto& f) { return f == 1; });
}

/// Writer
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
  , buffers_ready_{ nullptr }
  , zarr_{ zarr }
  , file_creator_{ zarr }
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
    delete[] buffers_ready_;
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
      frame_dims_.rows * frame_dims_.cols * bytes_of_type(pixel_type_);
    const auto frames_to_write = frames_per_chunk_ - frames_this_chunk;

    bytes_to_flush_ += frames_to_write * bytes_per_frame;
}

std::vector<size_t>
zarr::Writer::compress_buffers_() noexcept
{
    const auto nchunks = tiles_per_frame_();

    const size_t bytes_per_chunk = bytes_to_flush_ / nchunks;
    std::vector<size_t> buf_sizes;
    if (!blosc_compression_params_.has_value()) {
        for (auto& buf : chunk_buffers_) {
            buf_sizes.push_back(std::min(bytes_per_chunk, buf.size()));
        }
        return buf_sizes;
    }
    using namespace std::chrono_literals;

    buf_sizes.resize(nchunks);
    std::fill(buffers_ready_, buffers_ready_ + nchunks, false);

    TRACE("Compressing");

    const auto bytes_per_px = bytes_of_type(pixel_type_);

    std::scoped_lock lock(buffers_mutex_);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        auto& buf = chunk_buffers_.at(i);

        zarr_->push_to_job_queue([params = blosc_compression_params_.value(),
                                  buf = &buf,
                                  bytes_per_px,
                                  bytes_per_chunk,
                                  finished = buffers_ready_ + i,
                                  buf_size = buf_sizes.data() +
                                             i](std::string& err) -> bool {
            bool success = false;
            try {
                const auto tmp_size = bytes_per_chunk + BLOSC_MAX_OVERHEAD;
                std::vector<uint8_t> tmp(tmp_size);
                const auto nb =
                  blosc_compress_ctx(params.clevel,
                                     params.shuffle,
                                     bytes_per_px,
                                     bytes_per_chunk,
                                     buf->data(),
                                     tmp.data(),
                                     tmp_size,
                                     params.codec_id.c_str(),
                                     0 /* blocksize - 0:automatic */,
                                     1);
                if (nb > buf->size()) {
                    buf->resize(nb);
                }
                memcpy(buf->data(), tmp.data(), nb);
                *buf_size = nb;

                success = true;
            } catch (const std::exception& exc) {
                char msg[128];
                snprintf(
                  msg, sizeof(msg), "Failed to compress chunk: %s", exc.what());
                err = msg;
            } catch (...) {
                err = "Failed to compress chunk (unknown)";
            }
            *finished = true;

            return success;
        });
    }

    // wait for all threads to finish
    while (!std::all_of(buffers_ready_,
                        buffers_ready_ + nchunks,
                        [](const auto& b) { return b; })) {
        std::this_thread::sleep_for(500us);
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
