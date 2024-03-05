#include <stdexcept>
#include "writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <functional>
#include <latch>

namespace zarr = acquire::sink::zarr;

namespace {
/// Returns the index of the chunk in the lattice of chunks for the given frame
/// and dimension.
size_t
chunk_lattice_index(size_t frame_id,
                    size_t dimension_idx,
                    const std::vector<zarr::Dimension>& dims)
{
    CHECK(dimension_idx >= 2 && dimension_idx < dims.size());

    // the last dimension is a special case
    if (dimension_idx == dims.size() - 1) {
        size_t divisor = dims.back().chunk_size_px;
        for (auto i = 2; i < dims.size() - 1; ++i) {
            const auto& dim = dims.at(i);
            divisor *= dim.array_size_px;
        }

        CHECK(divisor);
        return frame_id / divisor;
    }

    size_t mod_divisor = 1, div_divisor = 1;
    for (auto i = 2; i <= dimension_idx; ++i) {
        const auto& dim = dims.at(i);
        mod_divisor *= dim.array_size_px;
        div_divisor *=
          (i < dimension_idx ? dim.array_size_px : dim.chunk_size_px);
    }

    CHECK(mod_divisor);
    CHECK(div_divisor);

    return (frame_id % mod_divisor) / div_divisor;
}

std::vector<size_t>
chunk_lattice_indices(size_t frame_id, const std::vector<zarr::Dimension>& dims)
{
    std::vector<size_t> indices;
    for (auto i = 2; i < dims.size(); ++i) {
        indices.push_back(chunk_lattice_index(frame_id, i, dims));
    }
    return indices;
}

size_t
chunk_group_offset(size_t frame_id, const std::vector<zarr::Dimension>& dims)
{
    std::vector<size_t> strides;
    strides.push_back(1);
    for (auto i = 0; i < dims.size() - 1; ++i) {
        const auto& dim = dims.at(i);
        CHECK(dim.chunk_size_px);
        const auto a = dim.array_size_px, c = dim.chunk_size_px;
        strides.push_back(strides.back() * ((a + c - 1) / c));
    }

    size_t offset = 0;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        const auto idx = chunk_lattice_index(frame_id, i, dims);
        const auto stride = strides.at(i);
        offset += idx * stride;
    }

    return offset;
}

size_t
chunk_internal_offset(size_t frame_id,
                      const std::vector<zarr::Dimension>& dims,
                      SampleType type)
{
    const auto tile_size =
      bytes_of_type(type) * dims.at(0).chunk_size_px * dims.at(1).chunk_size_px;
    auto offset = 0;
    std::vector<size_t> array_strides, chunk_strides;
    array_strides.push_back(1);
    chunk_strides.push_back(1);
    for (auto i = 2; i < dims.size(); ++i) {
        const auto& dim = dims.at(i);

        if (i < dims.size() - 1) {
            CHECK(dim.array_size_px);
        }
        CHECK(dim.chunk_size_px);

        const auto internal_idx =
          i == dims.size() - 1
            ? (frame_id / array_strides.back()) % dim.chunk_size_px
            : (frame_id / array_strides.back()) % dim.array_size_px %
                dim.chunk_size_px;
        offset += internal_idx * chunk_strides.back();

        array_strides.push_back(array_strides.back() * dim.array_size_px);
        chunk_strides.push_back(chunk_strides.back() * dim.chunk_size_px);
    }

    return offset * tile_size;
}
} // namespace

/// FileCreator
zarr::FileCreator::FileCreator(std::shared_ptr<common::ThreadPool> thread_pool)
  : thread_pool_{ thread_pool }
{
    EXPECT(thread_pool_, "Thread pool must not be null.");
}

bool
zarr::FileCreator::create_chunk_files(const fs::path& base_dir,
                                      const std::vector<Dimension>& dimensions,
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
        const auto n_dirs = n_chunks;

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_chunks; ++k) {
                paths.push(path / std::to_string(k));
            }
        }

        if (!make_dirs_(paths)) {
            return false;
        }
    }

    // create files
    {
        const auto& dim = dimensions.front();
        const auto n_chunks = common::chunks_along_dimension(dim);
        const auto n_files = n_chunks;

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_files; ++j) {
                paths.push(path / std::to_string(j));
            }
        }
    }

    return make_files_(paths, files);
}

bool
zarr::FileCreator::create_shard_files(const fs::path& base_dir,
                                      const std::vector<Dimension>& dimensions,
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
        const auto n_dirs = n_chunks / dim.shard_size_chunks;

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
    {
        const auto& dim = dimensions.front();
        const auto n_chunks = common::chunks_along_dimension(dim);
        const auto n_files = n_chunks / dim.shard_size_chunks;

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_files; ++j) {
                paths.push(path / std::to_string(j));
            }
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

std::optional<struct zarr::WriterConfig>
zarr::downsample(const WriterConfig& config)
{
    return std::nullopt;
    WriterConfig downsampled_config;

    for (const auto& dim : config.dimensions) {
        size_t downsampled_size =
          (dim.array_size_px + (dim.array_size_px % 2)) / 2;

        if (downsampled_size < dim.chunk_size_px) {
            return std::nullopt;
        }

        size_t shard_size_chunks = 0; // TODO (aliddell)

        downsampled_config.dimensions.push_back(
          Dimension(dim.name,
                    dim.kind,
                    downsampled_size,
                    dim.chunk_size_px,
                    dim.shard_size_chunks));
    }

    const ImageShape& image_shape = config.image_shape;
    ImageShape downsampled_shape = image_shape;

    downsampled_shape.dims.width =
      (image_shape.dims.width + (image_shape.dims.width % 2)) / 2;
    downsampled_shape.dims.height =
      (image_shape.dims.height + (image_shape.dims.height % 2)) / 2;
}

/// Writer
zarr::Writer::Writer(const WriterConfig& config,
                     std::shared_ptr<common::ThreadPool> thread_pool)
  : config_{ config }
  , thread_pool_{ thread_pool }
  , file_creator_{ thread_pool }
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , current_chunk_{ 0 }
{
    chunk_strides_.push_back(1);
    for (auto i = 0; i < config_.dimensions.size() - 1; ++i) {
        const auto& dim = config_.dimensions.at(i);
        chunks_per_dim_.push_back(common::chunks_along_dimension(dim));
        chunk_strides_.push_back(chunks_per_dim_.back() *
                                 chunk_strides_.back());
    }
    chunks_per_dim_.push_back(1);

    data_root_ = fs::path(config_.data_root);
}

bool
zarr::Writer::write(const VideoFrame* frame)
{
    validate_frame_(frame);
    if (chunk_buffers_.empty()) {
        make_buffers_();
    }

    // split the incoming frame into tiles and write them to the chunk buffers
    const auto& dimensions = config_.dimensions;

    const auto bytes_written = write_frame_to_chunks_(
      frame->data, frame->bytes_of_frame - sizeof(*frame));
    const auto bytes_of_frame = frame->bytes_of_frame - sizeof(*frame);
    CHECK(bytes_written == bytes_of_frame);
    bytes_to_flush_ += bytes_written;
    ++frames_written_;

    if (should_flush_()) {
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

const zarr::WriterConfig&
zarr::Writer::config() const noexcept
{
    return config_;
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

    EXPECT(frame->shape.dims.width == config_.image_shape.dims.width,
           "Expected frame to have %d columns. Got %d.",
           config_.image_shape.dims.width,
           frame->shape.dims.width);

    EXPECT(frame->shape.dims.height == config_.image_shape.dims.height,
           "Expected frame to have %d rows. Got %d.",
           config_.image_shape.dims.height,
           frame->shape.dims.height);

    EXPECT(frame->shape.type == config_.image_shape.type,
           "Expected frame to have pixel type %s. Got %s.",
           common::sample_type_to_string(config_.image_shape.type),
           common::sample_type_to_string(frame->shape.type));
}

void
zarr::Writer::make_buffers_() noexcept
{
    const size_t n_chunks = common::number_of_chunks(config_.dimensions);
    chunk_buffers_.resize(n_chunks); // no-op if already the correct size

    const auto bytes_per_chunk =
      common::bytes_per_chunk(config_.dimensions, config_.image_shape.type);

    for (auto& buf : chunk_buffers_) {
        buf.resize(bytes_per_chunk);
        std::fill_n(buf.begin(), bytes_per_chunk, 0);
    }
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
    if (!config_.compression_params.has_value()) {
        return;
    }

    TRACE("Compressing");

    BloscCompressionParams params = config_.compression_params.value();
    const auto bytes_per_px = bytes_of_type(config_.image_shape.type);

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
zarr::Writer::write_frame_to_chunks_(const uint8_t* buf, size_t buf_size)
{
    // break the frame into tiles and write them to the chunk buffers
    const auto image_shape = config_.image_shape;
    const auto bytes_per_px = bytes_of_type(image_shape.type);

    const auto frame_cols = image_shape.dims.width;
    const auto frame_rows = image_shape.dims.height;

    const auto& dimensions = config_.dimensions;
    const auto tile_cols = dimensions.at(0).chunk_size_px;
    const auto tile_rows = dimensions.at(1).chunk_size_px;
    const auto bytes_per_row = tile_cols * bytes_per_px;

    size_t bytes_written = 0;

    const auto n_tiles_x = (frame_cols + tile_cols - 1) / tile_cols;
    const auto n_tiles_y = (frame_rows + tile_rows - 1) / tile_rows;

    // don't take the frame id from the incoming frame, as the camera may have
    // dropped frames
    const auto frame_id = frames_written_;

    // offset among the chunks in the lattice
    const auto group_offset = chunk_group_offset(frame_id, dimensions);
    // offset within the chunk
    const auto chunk_offset =
      chunk_internal_offset(frame_id, dimensions, image_shape.type);

    for (auto i = 0; i < n_tiles_y; ++i) {
        // TODO (aliddell): we can optimize this when tiles_per_frame_x_ is 1
        for (auto j = 0; j < n_tiles_x; ++j) {
            const auto c = group_offset + i * n_tiles_x + j;
            auto& chunk = chunk_buffers_.at(c);
            auto chunk_it = chunk.begin() + chunk_offset;

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
                    EXPECT(region_stop <= buf_size, "Buffer overflow");

                    // copy region
                    EXPECT(chunk_it + nbytes <= chunk.end(), "Buffer overflow");
                    std::copy(buf + region_start, buf + region_stop, chunk_it);

                    bytes_written += (region_stop - region_start);
                }
                chunk_it += bytes_per_row;
            }
        }
    }

    return bytes_written;
}

void
zarr::Writer::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // fill in any unfilled chunks
    const auto bytes_per_chunk =
      common::bytes_per_chunk(config_.dimensions, config_.image_shape.type);

    // fill in last frames
    for (auto& chunk : chunk_buffers_) {
        EXPECT(bytes_per_chunk >= chunk.size(),
               "Chunk buffer size exceeds expected size.");
        std::fill_n(
          std::back_inserter(chunk), bytes_per_chunk - chunk.size(), 0);
    }

    // compress buffers and write out
    compress_buffers_();
    CHECK(flush_impl_());
    rollover_();

    // reset buffers
    make_buffers_();

    // reset state
    bytes_to_flush_ = 0;
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
    TestWriter(const zarr::WriterConfig& array_spec,
               std::shared_ptr<common::ThreadPool> thread_pool)
      : zarr::Writer(array_spec, thread_pool)
    {
    }

  private:
    bool should_flush_() const override { return false; }
    bool flush_impl_() override { return true; }
};

extern "C"
{
    acquire_export int unit_test__chunk_lattice_index()
    {
        int retval = 0;
        try {
            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
            dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
            dims.emplace_back("z", DimensionType_Space, 5, 2, 0);   // 3 chunks
            dims.emplace_back("c", DimensionType_Channel, 3, 2, 0); // 2 chunks
            dims.emplace_back(
              "t", DimensionType_Time, 0, 5, 0); // 5 timepoints / chunk

            CHECK(chunk_lattice_index(0, 2, dims) == 0);
            CHECK(chunk_lattice_index(0, 3, dims) == 0);
            CHECK(chunk_lattice_index(0, 4, dims) == 0);
            CHECK(chunk_lattice_index(1, 2, dims) == 0);
            CHECK(chunk_lattice_index(1, 3, dims) == 0);
            CHECK(chunk_lattice_index(1, 4, dims) == 0);
            CHECK(chunk_lattice_index(2, 2, dims) == 1);
            CHECK(chunk_lattice_index(2, 3, dims) == 0);
            CHECK(chunk_lattice_index(2, 4, dims) == 0);
            CHECK(chunk_lattice_index(3, 2, dims) == 1);
            CHECK(chunk_lattice_index(3, 3, dims) == 0);
            CHECK(chunk_lattice_index(3, 4, dims) == 0);
            CHECK(chunk_lattice_index(4, 2, dims) == 2);
            CHECK(chunk_lattice_index(4, 3, dims) == 0);
            CHECK(chunk_lattice_index(4, 4, dims) == 0);
            CHECK(chunk_lattice_index(5, 2, dims) == 0);
            CHECK(chunk_lattice_index(5, 3, dims) == 0);
            CHECK(chunk_lattice_index(5, 4, dims) == 0);
            CHECK(chunk_lattice_index(12, 2, dims) == 1);
            CHECK(chunk_lattice_index(12, 3, dims) == 1);
            CHECK(chunk_lattice_index(12, 4, dims) == 0);
            CHECK(chunk_lattice_index(19, 2, dims) == 2);
            CHECK(chunk_lattice_index(19, 3, dims) == 0);
            CHECK(chunk_lattice_index(19, 4, dims) == 0);
            CHECK(chunk_lattice_index(26, 2, dims) == 0);
            CHECK(chunk_lattice_index(26, 3, dims) == 1);
            CHECK(chunk_lattice_index(26, 4, dims) == 0);
            CHECK(chunk_lattice_index(33, 2, dims) == 1);
            CHECK(chunk_lattice_index(33, 3, dims) == 0);
            CHECK(chunk_lattice_index(33, 4, dims) == 0);
            CHECK(chunk_lattice_index(40, 2, dims) == 0);
            CHECK(chunk_lattice_index(40, 3, dims) == 1);
            CHECK(chunk_lattice_index(40, 4, dims) == 0);
            CHECK(chunk_lattice_index(47, 2, dims) == 1);
            CHECK(chunk_lattice_index(47, 3, dims) == 0);
            CHECK(chunk_lattice_index(47, 4, dims) == 0);
            CHECK(chunk_lattice_index(54, 2, dims) == 2);
            CHECK(chunk_lattice_index(54, 3, dims) == 0);
            CHECK(chunk_lattice_index(54, 4, dims) == 0);
            CHECK(chunk_lattice_index(61, 2, dims) == 0);
            CHECK(chunk_lattice_index(61, 3, dims) == 0);
            CHECK(chunk_lattice_index(61, 4, dims) == 0);
            CHECK(chunk_lattice_index(68, 2, dims) == 1);
            CHECK(chunk_lattice_index(68, 3, dims) == 0);
            CHECK(chunk_lattice_index(68, 4, dims) == 0);
            CHECK(chunk_lattice_index(74, 2, dims) == 2);
            CHECK(chunk_lattice_index(74, 3, dims) == 1);
            CHECK(chunk_lattice_index(74, 4, dims) == 0);
            CHECK(chunk_lattice_index(75, 2, dims) == 0);
            CHECK(chunk_lattice_index(75, 3, dims) == 0);
            CHECK(chunk_lattice_index(75, 4, dims) == 1);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return retval;
    }

    acquire_export int unit_test__chunk_group_offset()
    {
        int retval = 0;

        std::vector<zarr::Dimension> dims;
        dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
        dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
        dims.emplace_back("z", DimensionType_Space, 5, 2, 0);   // 3 chunks
        dims.emplace_back("c", DimensionType_Channel, 3, 2, 0); // 2 chunks
        dims.emplace_back(
          "t", DimensionType_Time, 0, 5, 0); // 5 timepoints / chunk

        try {
            CHECK(chunk_group_offset(0, dims) == 0);
            CHECK(chunk_group_offset(1, dims) == 0);
            CHECK(chunk_group_offset(2, dims) == 12);
            CHECK(chunk_group_offset(3, dims) == 12);
            CHECK(chunk_group_offset(4, dims) == 24);
            CHECK(chunk_group_offset(5, dims) == 0);
            CHECK(chunk_group_offset(6, dims) == 0);
            CHECK(chunk_group_offset(7, dims) == 12);
            CHECK(chunk_group_offset(8, dims) == 12);
            CHECK(chunk_group_offset(9, dims) == 24);
            CHECK(chunk_group_offset(10, dims) == 36);
            CHECK(chunk_group_offset(11, dims) == 36);
            CHECK(chunk_group_offset(12, dims) == 48);
            CHECK(chunk_group_offset(13, dims) == 48);
            CHECK(chunk_group_offset(14, dims) == 60);
            CHECK(chunk_group_offset(15, dims) == 0);
            CHECK(chunk_group_offset(16, dims) == 0);
            CHECK(chunk_group_offset(17, dims) == 12);
            CHECK(chunk_group_offset(18, dims) == 12);
            CHECK(chunk_group_offset(19, dims) == 24);
            CHECK(chunk_group_offset(20, dims) == 0);
            CHECK(chunk_group_offset(21, dims) == 0);
            CHECK(chunk_group_offset(22, dims) == 12);
            CHECK(chunk_group_offset(23, dims) == 12);
            CHECK(chunk_group_offset(24, dims) == 24);
            CHECK(chunk_group_offset(25, dims) == 36);
            CHECK(chunk_group_offset(26, dims) == 36);
            CHECK(chunk_group_offset(27, dims) == 48);
            CHECK(chunk_group_offset(28, dims) == 48);
            CHECK(chunk_group_offset(29, dims) == 60);
            CHECK(chunk_group_offset(30, dims) == 0);
            CHECK(chunk_group_offset(31, dims) == 0);
            CHECK(chunk_group_offset(32, dims) == 12);
            CHECK(chunk_group_offset(33, dims) == 12);
            CHECK(chunk_group_offset(34, dims) == 24);
            CHECK(chunk_group_offset(35, dims) == 0);
            CHECK(chunk_group_offset(36, dims) == 0);
            CHECK(chunk_group_offset(37, dims) == 12);
            CHECK(chunk_group_offset(38, dims) == 12);
            CHECK(chunk_group_offset(39, dims) == 24);
            CHECK(chunk_group_offset(40, dims) == 36);
            CHECK(chunk_group_offset(41, dims) == 36);
            CHECK(chunk_group_offset(42, dims) == 48);
            CHECK(chunk_group_offset(43, dims) == 48);
            CHECK(chunk_group_offset(44, dims) == 60);
            CHECK(chunk_group_offset(45, dims) == 0);
            CHECK(chunk_group_offset(46, dims) == 0);
            CHECK(chunk_group_offset(47, dims) == 12);
            CHECK(chunk_group_offset(48, dims) == 12);
            CHECK(chunk_group_offset(49, dims) == 24);
            CHECK(chunk_group_offset(50, dims) == 0);
            CHECK(chunk_group_offset(51, dims) == 0);
            CHECK(chunk_group_offset(52, dims) == 12);
            CHECK(chunk_group_offset(53, dims) == 12);
            CHECK(chunk_group_offset(54, dims) == 24);
            CHECK(chunk_group_offset(55, dims) == 36);
            CHECK(chunk_group_offset(56, dims) == 36);
            CHECK(chunk_group_offset(57, dims) == 48);
            CHECK(chunk_group_offset(58, dims) == 48);
            CHECK(chunk_group_offset(59, dims) == 60);
            CHECK(chunk_group_offset(60, dims) == 0);
            CHECK(chunk_group_offset(61, dims) == 0);
            CHECK(chunk_group_offset(62, dims) == 12);
            CHECK(chunk_group_offset(63, dims) == 12);
            CHECK(chunk_group_offset(64, dims) == 24);
            CHECK(chunk_group_offset(65, dims) == 0);
            CHECK(chunk_group_offset(66, dims) == 0);
            CHECK(chunk_group_offset(67, dims) == 12);
            CHECK(chunk_group_offset(68, dims) == 12);
            CHECK(chunk_group_offset(69, dims) == 24);
            CHECK(chunk_group_offset(70, dims) == 36);
            CHECK(chunk_group_offset(71, dims) == 36);
            CHECK(chunk_group_offset(72, dims) == 48);
            CHECK(chunk_group_offset(73, dims) == 48);
            CHECK(chunk_group_offset(74, dims) == 60);
            CHECK(chunk_group_offset(75, dims) == 0);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__chunk_internal_offset()
    {
        int retval = 0;

        std::vector<zarr::Dimension> dims;
        dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
        dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
        dims.emplace_back("z", DimensionType_Space, 5, 2, 0);   // 3 chunks
        dims.emplace_back("c", DimensionType_Channel, 3, 2, 0); // 2 chunks
        dims.emplace_back(
          "t", DimensionType_Time, 0, 5, 0); // 5 timepoints / chunk

        try {
            CHECK(chunk_internal_offset(0, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(1, dims, SampleType_u16) == 512);
            CHECK(chunk_internal_offset(2, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(3, dims, SampleType_u16) == 512);
            CHECK(chunk_internal_offset(4, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(5, dims, SampleType_u16) == 1024);
            CHECK(chunk_internal_offset(6, dims, SampleType_u16) == 1536);
            CHECK(chunk_internal_offset(7, dims, SampleType_u16) == 1024);
            CHECK(chunk_internal_offset(8, dims, SampleType_u16) == 1536);
            CHECK(chunk_internal_offset(9, dims, SampleType_u16) == 1024);
            CHECK(chunk_internal_offset(10, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(11, dims, SampleType_u16) == 512);
            CHECK(chunk_internal_offset(12, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(13, dims, SampleType_u16) == 512);
            CHECK(chunk_internal_offset(14, dims, SampleType_u16) == 0);
            CHECK(chunk_internal_offset(15, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(16, dims, SampleType_u16) == 2560);
            CHECK(chunk_internal_offset(17, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(18, dims, SampleType_u16) == 2560);
            CHECK(chunk_internal_offset(19, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(20, dims, SampleType_u16) == 3072);
            CHECK(chunk_internal_offset(21, dims, SampleType_u16) == 3584);
            CHECK(chunk_internal_offset(22, dims, SampleType_u16) == 3072);
            CHECK(chunk_internal_offset(23, dims, SampleType_u16) == 3584);
            CHECK(chunk_internal_offset(24, dims, SampleType_u16) == 3072);
            CHECK(chunk_internal_offset(25, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(26, dims, SampleType_u16) == 2560);
            CHECK(chunk_internal_offset(27, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(28, dims, SampleType_u16) == 2560);
            CHECK(chunk_internal_offset(29, dims, SampleType_u16) == 2048);
            CHECK(chunk_internal_offset(30, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(31, dims, SampleType_u16) == 4608);
            CHECK(chunk_internal_offset(32, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(33, dims, SampleType_u16) == 4608);
            CHECK(chunk_internal_offset(34, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(35, dims, SampleType_u16) == 5120);
            CHECK(chunk_internal_offset(36, dims, SampleType_u16) == 5632);
            CHECK(chunk_internal_offset(37, dims, SampleType_u16) == 5120);
            CHECK(chunk_internal_offset(38, dims, SampleType_u16) == 5632);
            CHECK(chunk_internal_offset(39, dims, SampleType_u16) == 5120);
            CHECK(chunk_internal_offset(40, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(41, dims, SampleType_u16) == 4608);
            CHECK(chunk_internal_offset(42, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(43, dims, SampleType_u16) == 4608);
            CHECK(chunk_internal_offset(44, dims, SampleType_u16) == 4096);
            CHECK(chunk_internal_offset(45, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(46, dims, SampleType_u16) == 6656);
            CHECK(chunk_internal_offset(47, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(48, dims, SampleType_u16) == 6656);
            CHECK(chunk_internal_offset(49, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(50, dims, SampleType_u16) == 7168);
            CHECK(chunk_internal_offset(51, dims, SampleType_u16) == 7680);
            CHECK(chunk_internal_offset(52, dims, SampleType_u16) == 7168);
            CHECK(chunk_internal_offset(53, dims, SampleType_u16) == 7680);
            CHECK(chunk_internal_offset(54, dims, SampleType_u16) == 7168);
            CHECK(chunk_internal_offset(55, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(56, dims, SampleType_u16) == 6656);
            CHECK(chunk_internal_offset(57, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(58, dims, SampleType_u16) == 6656);
            CHECK(chunk_internal_offset(59, dims, SampleType_u16) == 6144);
            CHECK(chunk_internal_offset(60, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(61, dims, SampleType_u16) == 8704);
            CHECK(chunk_internal_offset(62, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(63, dims, SampleType_u16) == 8704);
            CHECK(chunk_internal_offset(64, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(65, dims, SampleType_u16) == 9216);
            CHECK(chunk_internal_offset(66, dims, SampleType_u16) == 9728);
            CHECK(chunk_internal_offset(67, dims, SampleType_u16) == 9216);
            CHECK(chunk_internal_offset(68, dims, SampleType_u16) == 9728);
            CHECK(chunk_internal_offset(69, dims, SampleType_u16) == 9216);
            CHECK(chunk_internal_offset(70, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(71, dims, SampleType_u16) == 8704);
            CHECK(chunk_internal_offset(72, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(73, dims, SampleType_u16) == 8704);
            CHECK(chunk_internal_offset(74, dims, SampleType_u16) == 8192);
            CHECK(chunk_internal_offset(75, dims, SampleType_u16) == 0);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__file_creator__create_chunk_files()
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
            CHECK(file_creator.create_chunk_files(base_dir, dims, files));

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

    acquire_export int unit_test__file_creator__create_shard_files()
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
            CHECK(file_creator.create_shard_files(base_dir, dims, files));

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

            zarr::WriterConfig array_spec = {
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
