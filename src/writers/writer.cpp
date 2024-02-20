#include <stdexcept>
#include "writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <functional>
#include <latch>

namespace zarr = acquire::sink::zarr;

/// FileCreator
zarr::FileCreator::FileCreator(std::shared_ptr<common::ThreadPool> thread_pool)
  : thread_pool_{ thread_pool }
{
    EXPECT(thread_pool_, "Thread pool must not be null.");
}

bool
zarr::FileCreator::create_files(const fs::path& base_dir,
                                const std::vector<Dimension>& dimensions,
                                bool make_shards,
                                std::vector<file>& files)
{
    std::queue<fs::path> paths;
    paths.push(base_dir);

    if (!make_dirs_(paths)) {
        return false;
    }

    // create directories
    for (auto i = dimensions.size() - 2; i >= 1; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_chunks = common::chunks_along_dimension(dim);
        const auto n_dirs =
          make_shards ? n_chunks / dim.shard_size_chunks : n_chunks;

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_dirs; ++k) {
                paths.push(path / std::to_string(k));
            }
        }

        if (!make_dirs_(paths)) {
            return false;
        }
    }

    // create files
    auto n_paths = paths.size();
    const auto n_chunks = common::chunks_along_dimension(dimensions.front());
    const auto n_files =
      make_shards ? n_chunks / dimensions.front().shard_size_chunks : n_chunks;
    for (auto i = 0; i < n_paths; ++i) {
        const auto path = paths.front();
        paths.pop();
        for (auto j = 0; j < n_files; ++j) {
            paths.push(path / std::to_string(j));
        }
    }

    return make_files_(paths, files);
}

bool
zarr::FileCreator::make_dirs_(std::queue<fs::path>& dir_paths)
{
    if (dir_paths.empty()) {
        return true;
    }

    std::atomic<bool> success = true;

    const auto n_dirs = dir_paths.size();
    std::latch latch(n_dirs);

    for (auto i = 0; i < n_dirs; ++i) {
        const auto dirname = dir_paths.front();
        dir_paths.pop();

        thread_pool_->push_to_job_queue(
          [dirname, &latch, &success](std::string& err) -> bool {
              try {
                  if (fs::exists(dirname)) {
                      EXPECT(fs::is_directory(dirname),
                             "'%s' exists but is not a directory.",
                             dirname.c_str());
                  } else if (success) {
                      EXPECT(fs::create_directories(dirname),
                             "Not creating directory '%s': another job failed.",
                             dirname.c_str());
                  }
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': %s.",
                           dirname.string().c_str(),
                           exc.what());
                  err = buf;
                  success = false;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': (unknown).",
                           dirname.string().c_str());
                  err = buf;
                  success = false;
              }

              latch.count_down();
              return success;
          });

        dir_paths.push(dirname);
    }

    latch.wait();

    return success;
}

bool
zarr::FileCreator::make_files_(std::queue<fs::path>& file_paths,
                               std::vector<struct file>& files)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<bool> success = true;

    const auto n_files = file_paths.size();
    files.resize(n_files);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths.front();
        file_paths.pop();

        struct file* pfile = files.data() + i;

        thread_pool_->push_to_job_queue(
          [filename, pfile, &latch, &success](std::string& err) -> bool {
              try {
                  CHECK(success);
                  EXPECT(file_create(pfile,
                                     filename.string().c_str(),
                                     filename.string().length()),
                         "Failed to open file: '%s'",
                         filename.string().c_str());
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': %s.",
                           filename.string().c_str(),
                           exc.what());
                  err = buf;
                  success = false;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': (unknown).",
                           filename.string().c_str());
                  err = buf;
                  success = false;
              }

              latch.count_down();
              return success;
          });
    }

    latch.wait();

    return success;
}

/// Writer
zarr::Writer::Writer(const ArraySpec& array_spec,
                     std::shared_ptr<common::ThreadPool> thread_pool)
  : array_spec_{ array_spec }
  , thread_pool_{ thread_pool }
  , file_creator_{ thread_pool }
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , current_chunk_{ 0 }
  , chunk_offset_{ 0 }
  , should_flush_{ false }
  , array_counters_(array_spec_.dimensions.size() - 2, 0)
  , chunk_counters_(array_spec_.dimensions.size() - 2, 0)
{
    chunk_strides_.push_back(1);
    for (auto i = 0; i < array_spec_.dimensions.size() - 1; ++i) {
        const auto& dim = array_spec_.dimensions.at(i);
        chunks_per_dim_.push_back(common::chunks_along_dimension(dim));
        chunk_strides_.push_back(chunks_per_dim_.back() *
                                 chunk_strides_.back());
    }
    chunks_per_dim_.push_back(1);

    data_root_ = fs::path(array_spec_.data_root);
}

bool
zarr::Writer::write(const VideoFrame* frame)
{
    validate_frame_(frame);

    if (chunk_buffers_.empty()) {
        make_buffers_();
    }

    // split the incoming frame into tiles and write them to the chunk buffers
    const auto& dimensions = array_spec_.dimensions;

    const auto bytes_written = write_frame_to_chunks_(
      frame->data, frame->bytes_of_frame - sizeof(*frame));
    CHECK(bytes_written == frame->bytes_of_frame - sizeof(*frame));
    bytes_to_flush_ += bytes_written;

    // increment chunk offset

    increment_counters_();

    if (should_flush_) {
        flush_();
    }

    return true;
}

void
zarr::Writer::finalize()
{
    finalize_chunks_();
    flush_();
    close_files_();
}

uint32_t
zarr::Writer::frames_written() const noexcept
{
    return frames_written_;
}

void
zarr::Writer::validate_frame_(const VideoFrame* frame)
{
    CHECK(frame);

    EXPECT(frame->shape.dims.width == array_spec_.image_shape.dims.width,
           "Expected frame to have %d columns. Got %d.",
           array_spec_.image_shape.dims.width,
           frame->shape.dims.width);

    EXPECT(frame->shape.dims.height == array_spec_.image_shape.dims.height,
           "Expected frame to have %d rows. Got %d.",
           array_spec_.image_shape.dims.height,
           frame->shape.dims.height);

    EXPECT(frame->shape.type == array_spec_.image_shape.type,
           "Expected frame to have pixel type %s. Got %s.",
           common::sample_type_to_string(array_spec_.image_shape.type),
           common::sample_type_to_string(frame->shape.type));
}

void
zarr::Writer::make_buffers_() noexcept
{
    size_t n_chunks = common::number_of_chunks(array_spec_.dimensions);

    const auto bytes_per_chunk = common::bytes_per_chunk(
      array_spec_.dimensions, array_spec_.image_shape.type);

    const auto bytes_to_reserve =
      bytes_per_chunk +
      (array_spec_.compression_params.has_value() ? BLOSC_MAX_OVERHEAD : 0);

    for (auto i = 0; i < n_chunks; ++i) {
        chunk_buffers_.emplace_back();
        chunk_buffers_.back().reserve(bytes_to_reserve);
    }
}

void
zarr::Writer::fill_chunks_(uint32_t dim_idx)
{
    const auto dimensions = array_spec_.dimensions;
    const auto& dim = dimensions.at(dim_idx);

    auto tiles_to_fill = 1;
    for (auto i = 2; i < dim_idx + 2 - 1; ++i) {
        tiles_to_fill *= dimensions.at(i).chunk_size_px;
    }
    tiles_to_fill *=
      (dimensions.at(dim_idx + 2).chunk_size_px - chunk_counters_.at(dim_idx));

    const auto bytes_per_tile = dimensions.at(0).chunk_size_px *
                                dimensions.at(1).chunk_size_px *
                                bytes_of_type(array_spec_.image_shape.type);
    const auto bytes_to_fill = tiles_to_fill * bytes_per_tile;

    for (auto i = chunk_offset_; i < chunk_offset_ + tiles_per_frame_(); ++i) {
        auto& chunk = chunk_buffers_.at(i);
        std::fill_n(std::back_inserter(chunk), bytes_to_fill, 0);
    }
    chunk_counters_.at(dim_idx) = 0;
    chunk_offset_ = 0;
    bytes_to_flush_ += chunk_buffers_.size() * bytes_to_fill;
}

void
zarr::Writer::finalize_chunks_() noexcept
{
    //    const auto frames_this_chunk = frames_written_ % frames_per_chunk_();
    //
    //    // don't write zeros if we have written less than one full chunk or if
    //    // the last frame written was the final frame in its chunk
    //    if (frames_written_ < frames_per_chunk_() || frames_this_chunk == 0) {
    //        return;
    //    }
    //
    //    const auto bytes_per_frame =
    //      common::bytes_of_image(array_spec_.image_shape);
    //    const auto frames_to_write = frames_per_chunk_() - frames_this_chunk;
    //
    //    const auto bytes_to_fill =
    //      frames_to_write * common::bytes_per_tile(tile_dims_, pixel_type_);
    //    for (auto& chunk : chunk_buffers_) {
    //        std::fill_n(std::back_inserter(chunk), bytes_to_fill, 0);
    //    }
    //
    //    bytes_to_flush_ += frames_to_write * bytes_per_frame;
}

void
zarr::Writer::compress_buffers_() noexcept
{
    const auto n_chunks = chunk_buffers_.size();

    const size_t bytes_per_chunk = bytes_to_flush_ / n_chunks;
    if (!array_spec_.compression_params.has_value()) {
        return;
    }

    TRACE("Compressing");

    BloscCompressionParams params = array_spec_.compression_params.value();
    const auto bytes_per_px = bytes_of_type(array_spec_.image_shape.type);

    std::scoped_lock lock(buffers_mutex_);
    std::latch latch(chunk_buffers_.size());
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        auto& chunk = chunk_buffers_.at(i);

        thread_pool_->push_to_job_queue(
          [&params, buf = &chunk, bytes_per_px, bytes_per_chunk, &latch](
            std::string& err) -> bool {
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
          });
    }

    // wait for all threads to finish
    latch.wait();
}

size_t
zarr::Writer::write_frame_to_chunks_(const uint8_t* buf,
                                     size_t buf_size) noexcept
{
    // break the frame into tiles and write them to the chunk buffers
    const auto image_shape = array_spec_.image_shape;
    const auto bytes_per_px = bytes_of_type(image_shape.type);

    const auto frame_cols = image_shape.dims.width;
    const auto frame_rows = image_shape.dims.height;

    const auto frames_this_chunk = frames_written_ % frames_per_chunk_();

    const auto tile_cols = array_spec_.dimensions.at(0).chunk_size_px;
    const auto tile_rows = array_spec_.dimensions.at(1).chunk_size_px;
    const auto bytes_per_row = tile_cols * bytes_per_px;
    const auto bytes_per_tile = tile_rows * tile_cols * bytes_per_px;

    size_t bytes_written = 0;

    const auto tiles_per_frame_x =
      common::chunks_along_dimension(array_spec_.dimensions.at(0));
    const auto tiles_per_frame_y =
      common::chunks_along_dimension(array_spec_.dimensions.at(1));

    for (auto i = 0; i < tiles_per_frame_y; ++i) {
        // TODO (aliddell): we can optimize this when tiles_per_frame_x_
        for (auto j = 0; j < tiles_per_frame_x; ++j) {
            size_t offset = bytes_per_tile * frames_this_chunk;

            const auto c = chunk_offset_ + i * tiles_per_frame_x + j;
            auto& chunk = chunk_buffers_.at(c);

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

                    // copy region
                    std::copy(buf + region_start,
                              buf + region_stop,
                              std::back_inserter(chunk));

                    // fill remainder with zeros
                    std::fill_n(
                      std::back_inserter(chunk), bytes_per_row - nbytes, 0);

                    bytes_written += bytes_per_row;
                } else {
                    std::fill_n(std::back_inserter(chunk), bytes_per_row, 0);
                    bytes_written += bytes_per_row;
                }
                offset += tile_cols * bytes_per_px;
            }
        }
    }

    return bytes_written;
}

size_t
zarr::Writer::n_chunks_() const noexcept
{
    return common::number_of_chunks(array_spec_.dimensions);
}

size_t
zarr::Writer::tiles_per_frame_() const noexcept
{
    const auto tiles_per_frame_x =
      common::chunks_along_dimension(array_spec_.dimensions.at(0));
    const auto tiles_per_frame_y =
      common::chunks_along_dimension(array_spec_.dimensions.at(1));

    return tiles_per_frame_x * tiles_per_frame_y;
}

size_t
zarr::Writer::frames_per_chunk_() const noexcept
{
    size_t frames_per_chunk = 1;
    for (auto i = 2; i < array_spec_.dimensions.size(); ++i) {
        frames_per_chunk *= array_spec_.dimensions.at(i).chunk_size_px;
    }

    return frames_per_chunk;
}

void
zarr::Writer::increment_counters_()
{
    // to be called after writing a frame to the chunk buffers
    ++frames_written_;
    const auto& dimensions = array_spec_.dimensions;

    bool increment_chunk_offset = false;

    // increment chunk counter(s)
    for (auto i = 0; i < chunk_counters_.size(); ++i) {
        const auto counter = ++chunk_counters_.at(i);
        const auto threshold = dimensions.at(i + 2).chunk_size_px;
        if (counter == threshold) {
            chunk_counters_.at(i) = 0;
            increment_chunk_offset = true;
        } else {
            break;
        }
    }

    if (increment_chunk_offset) {
        chunk_offset_ =
          (chunk_offset_ + tiles_per_frame_()) % chunk_buffers_.size();
    }

    // increment array counter(s)
    for (auto i = 0; i < array_counters_.size(); ++i) {
        const auto counter = ++array_counters_.at(i);
        const auto threshold = i == array_counters_.size() - 1
                                 ? dimensions.back().chunk_size_px
                                 : dimensions.at(i + 2).array_size_px;
        if (counter == threshold) {
            array_counters_.at(i) = 0;
        } else {
            break;
        }
    }

    // fill ragged dimensions
    for (auto i = 0; i < chunk_counters_.size(); ++i) {
        const auto array = array_counters_.at(i), chunk = chunk_counters_.at(i);
        if (array != 0) {
            break;
        }

        if (chunk != 0) {
            LOG("Ragged dimension of size %d", chunk_counters_.at(i));
            fill_chunks_(i);
        }
    }

    should_flush_ = std::all_of(array_counters_.begin(),
                                array_counters_.end(),
                                [](const auto& c) { return c == 0; });
}

void
zarr::Writer::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // fill in any unfilled chunks
    const auto bytes_per_chunk = common::bytes_per_chunk(
      array_spec_.dimensions, array_spec_.image_shape.type);

    // fill in last frames
    for (auto& chunk : chunk_buffers_) {
        EXPECT(bytes_per_chunk >= chunk.size(),
               "Chunk buffer size exceeds expected size.");
        std::fill_n(
          std::back_inserter(chunk), bytes_per_chunk - chunk.size(), 0);
    }

    // compress buffers and write out
    compress_buffers_();
    flush_impl_();
    rollover_();

    // reset buffers
    const auto bytes_to_reserve =
      bytes_per_chunk +
      (array_spec_.compression_params.has_value() ? BLOSC_MAX_OVERHEAD : 0);

    for (auto& buf : chunk_buffers_) {
        buf.clear();
        buf.reserve(bytes_to_reserve);
    }

    // reset state
    bytes_to_flush_ = 0;
    chunk_offset_ = 0;
    should_flush_ = false;
}

void
zarr::Writer::close_files_()
{
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

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

namespace common = zarr::common;

class TestWriter : public zarr::Writer
{
  public:
    TestWriter(const zarr::ArraySpec& array_spec,
               std::shared_ptr<common::ThreadPool> thread_pool)
      : zarr::Writer(array_spec, thread_pool)
    {
    }

    void flush_impl_() override {}
};

extern "C"
{
    acquire_export int unit_test__file_creator__make_chunk_files()
    {
        const auto base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });
            zarr::FileCreator file_creator{ thread_pool };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 10, 2, 0); // 5 chunks
            dims.emplace_back("y", DimensionType_Space, 4, 2, 0);  // 2 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 0, 3, 0); // 3 timepoints per chunk

            std::vector<struct file> files;
            CHECK(file_creator.create_files(base_dir, dims, false, files));

            CHECK(files.size() == 5 * 2);
            std::for_each(files.begin(), files.end(), [](const struct file& f) {
                file_close(const_cast<struct file*>(&f));
            });

            CHECK(fs::is_directory(base_dir));
            for (auto y = 0; y < 2; ++y) {
                CHECK(fs::is_directory(base_dir / std::to_string(y)));
                for (auto x = 0; x < 5; ++x) {
                    CHECK(fs::is_regular_file(base_dir / std::to_string(y) /
                                              std::to_string(x)));
                }
            }
            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        return retval;
    }

    acquire_export int unit_test__file_creator__make_shard_files()
    {
        const auto base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });
            zarr::FileCreator file_creator{ thread_pool };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, 10, 2, 5); // 5 chunks, 1 shard
            dims.emplace_back(
              "y", DimensionType_Space, 4, 2, 1); // 2 chunks, 2 shards
            dims.emplace_back(
              "z", DimensionType_Space, 8, 2, 2); // 4 chunks, 2 shards

            std::vector<struct file> files;
            CHECK(file_creator.create_files(base_dir, dims, true, files));

            CHECK(files.size() == 2);
            std::for_each(files.begin(), files.end(), [](const struct file& f) {
                file_close(const_cast<struct file*>(&f));
            });

            CHECK(fs::is_directory(base_dir));
            for (auto y = 0; y < 2; ++y) {
                CHECK(fs::is_directory(base_dir / std::to_string(y)));
                for (auto x = 0; x < 1; ++x) {
                    CHECK(fs::is_regular_file(base_dir / std::to_string(y) /
                                              std::to_string(x)));
                }
            }

            // cleanup
            fs::remove_all(base_dir);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        return retval;
    }

    acquire_export int unit_test__writer__write_frame_to_chunks()
    {
        const auto base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
            dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
            dims.emplace_back("z", DimensionType_Space, 2, 1, 0);   // 2 chunks
            dims.emplace_back("c", DimensionType_Channel, 1, 1, 0); // 1 chunk
            dims.emplace_back(
              "t", DimensionType_Time, 2, 1, 0); // 1 timepoint/chunk

            ImageShape shape {
                .dims = {
                    .width = 64,
                    .height = 48,
                },
                .type = SampleType_u16,
            };

            zarr::ArraySpec array_spec = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            TestWriter writer(array_spec, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48 * 2);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48 * 2;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48 * 2);

            for (auto i = 0; i < 2 * 1 * 2; ++i) {
                CHECK(writer.write(frame));
            }

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        if (frame) {
            free(frame);
        }
        return retval;
    }
};
#endif
