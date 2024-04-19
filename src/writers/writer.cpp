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

/// Find the offset in the array of chunks for the given frame.
size_t
tile_group_offset(size_t frame_id, const std::vector<zarr::Dimension>& dims)
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

/// Find the offset inside a chunk for the given frame.
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
        CHECK(array_strides.back());

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
} // end ::{anonymous} namespace

bool
zarr::downsample(const ArrayConfig& config, ArrayConfig& downsampled_config)
{
    // downsample dimensions
    downsampled_config.dimensions.clear();
    for (const auto& dim : config.dimensions) {
        if (dim.kind == DimensionType_Channel) { // don't downsample channels
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

    // downsample image_shape
    downsampled_config.image_shape = config.image_shape;

    downsampled_config.image_shape.dims.width =
      downsampled_config.dimensions.at(0).array_size_px;
    downsampled_config.image_shape.dims.height =
      downsampled_config.dimensions.at(1).array_size_px;

    downsampled_config.image_shape.strides.height =
      downsampled_config.image_shape.dims.width;
    downsampled_config.image_shape.strides.planes =
      downsampled_config.image_shape.dims.width *
      downsampled_config.image_shape.dims.height;

    // data root needs updated
    fs::path downsampled_data_root = config.data_root;
    // increment the array number in the group
    downsampled_data_root.replace_filename(
      std::to_string(std::stoi(downsampled_data_root.filename()) + 1));
    downsampled_config.data_root = downsampled_data_root.string();

    // copy the Blosc compression parameters
    downsampled_config.compression_params = config.compression_params;

    // can we downsample downsampled_config?
    for (auto i = 0; i < config.dimensions.size(); ++i) {
        // downsampling made the chunk size strictly smaller
        const auto& dim = config.dimensions.at(i);
        const auto& downsampled_dim = downsampled_config.dimensions.at(i);

        if (dim.chunk_size_px > downsampled_dim.chunk_size_px) {
            return false;
        }
    }

    return true;
}

/// Writer
zarr::Writer::Writer(const ArrayConfig& config,
                     std::shared_ptr<common::ThreadPool> thread_pool)
  : config_{ config }
  , thread_pool_{ thread_pool }
  , file_creator_{ thread_pool }
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , is_finalizing_{ false }
{
    data_root_ = config_.data_root;
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
    is_finalizing_ = true;
    flush_();
    close_files_();
    is_finalizing_ = false;
}

const zarr::ArrayConfig&
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
zarr::Writer::make_buffers_() noexcept
{
    const size_t n_chunks =
      common::number_of_chunks_in_memory(config_.dimensions);
    chunk_buffers_.resize(n_chunks); // no-op if already the correct size

    const auto bytes_per_chunk =
      common::bytes_per_chunk(config_.dimensions, config_.image_shape.type);

    for (auto& buf : chunk_buffers_) {
        buf.resize(bytes_per_chunk);
        std::fill_n(buf.begin(), bytes_per_chunk, 0);
    }
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

    CHECK(tile_cols);
    const auto n_tiles_x = (frame_cols + tile_cols - 1) / tile_cols;
    CHECK(tile_rows);
    const auto n_tiles_y = (frame_rows + tile_rows - 1) / tile_rows;

    // don't take the frame id from the incoming frame, as the camera may have
    // dropped frames
    const auto frame_id = frames_written_;

    // offset among the chunks in the lattice
    const auto group_offset = tile_group_offset(frame_id, dimensions);
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

bool
zarr::Writer::should_flush_() const
{
    const auto& dims = config_.dimensions;
    size_t frames_before_flush = dims.back().chunk_size_px;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        frames_before_flush *= dims.at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

void
zarr::Writer::compress_buffers_() noexcept
{
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

        thread_pool_->push_to_job_queue([&params,
                                         buf = &chunk,
                                         bytes_per_px,
                                         &latch](std::string& err) -> bool {
            bool success = false;
            const size_t bytes_of_chunk = buf->size();

            try {
                const auto tmp_size = bytes_of_chunk + BLOSC_MAX_OVERHEAD;
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
                snprintf(
                  msg, sizeof(msg), "Failed to compress chunk: %s", exc.what());
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

void
zarr::Writer::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // compress buffers and write out
    compress_buffers_();
    CHECK(flush_impl_());

    if (should_rollover_()) {
        rollover_();
    }

    // reset buffers
    make_buffers_();

    // reset state
    bytes_to_flush_ = 0;
}

void
zarr::Writer::close_files_()
{
    for (auto* sink : sinks_) {
        sink_close(sink);
    }
    sinks_.clear();
}

void
zarr::Writer::rollover_()
{
    TRACE("Rolling over");

    close_files_();
    ++append_chunk_index_;
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
    TestWriter(const zarr::ArrayConfig& array_spec,
               std::shared_ptr<common::ThreadPool> thread_pool)
      : zarr::Writer(array_spec, thread_pool)
    {
    }

  private:
    bool should_rollover_() const override { return false; }
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

    acquire_export int unit_test__tile_group_offset()
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
            CHECK(tile_group_offset(0, dims) == 0);
            CHECK(tile_group_offset(1, dims) == 0);
            CHECK(tile_group_offset(2, dims) == 12);
            CHECK(tile_group_offset(3, dims) == 12);
            CHECK(tile_group_offset(4, dims) == 24);
            CHECK(tile_group_offset(5, dims) == 0);
            CHECK(tile_group_offset(6, dims) == 0);
            CHECK(tile_group_offset(7, dims) == 12);
            CHECK(tile_group_offset(8, dims) == 12);
            CHECK(tile_group_offset(9, dims) == 24);
            CHECK(tile_group_offset(10, dims) == 36);
            CHECK(tile_group_offset(11, dims) == 36);
            CHECK(tile_group_offset(12, dims) == 48);
            CHECK(tile_group_offset(13, dims) == 48);
            CHECK(tile_group_offset(14, dims) == 60);
            CHECK(tile_group_offset(15, dims) == 0);
            CHECK(tile_group_offset(16, dims) == 0);
            CHECK(tile_group_offset(17, dims) == 12);
            CHECK(tile_group_offset(18, dims) == 12);
            CHECK(tile_group_offset(19, dims) == 24);
            CHECK(tile_group_offset(20, dims) == 0);
            CHECK(tile_group_offset(21, dims) == 0);
            CHECK(tile_group_offset(22, dims) == 12);
            CHECK(tile_group_offset(23, dims) == 12);
            CHECK(tile_group_offset(24, dims) == 24);
            CHECK(tile_group_offset(25, dims) == 36);
            CHECK(tile_group_offset(26, dims) == 36);
            CHECK(tile_group_offset(27, dims) == 48);
            CHECK(tile_group_offset(28, dims) == 48);
            CHECK(tile_group_offset(29, dims) == 60);
            CHECK(tile_group_offset(30, dims) == 0);
            CHECK(tile_group_offset(31, dims) == 0);
            CHECK(tile_group_offset(32, dims) == 12);
            CHECK(tile_group_offset(33, dims) == 12);
            CHECK(tile_group_offset(34, dims) == 24);
            CHECK(tile_group_offset(35, dims) == 0);
            CHECK(tile_group_offset(36, dims) == 0);
            CHECK(tile_group_offset(37, dims) == 12);
            CHECK(tile_group_offset(38, dims) == 12);
            CHECK(tile_group_offset(39, dims) == 24);
            CHECK(tile_group_offset(40, dims) == 36);
            CHECK(tile_group_offset(41, dims) == 36);
            CHECK(tile_group_offset(42, dims) == 48);
            CHECK(tile_group_offset(43, dims) == 48);
            CHECK(tile_group_offset(44, dims) == 60);
            CHECK(tile_group_offset(45, dims) == 0);
            CHECK(tile_group_offset(46, dims) == 0);
            CHECK(tile_group_offset(47, dims) == 12);
            CHECK(tile_group_offset(48, dims) == 12);
            CHECK(tile_group_offset(49, dims) == 24);
            CHECK(tile_group_offset(50, dims) == 0);
            CHECK(tile_group_offset(51, dims) == 0);
            CHECK(tile_group_offset(52, dims) == 12);
            CHECK(tile_group_offset(53, dims) == 12);
            CHECK(tile_group_offset(54, dims) == 24);
            CHECK(tile_group_offset(55, dims) == 36);
            CHECK(tile_group_offset(56, dims) == 36);
            CHECK(tile_group_offset(57, dims) == 48);
            CHECK(tile_group_offset(58, dims) == 48);
            CHECK(tile_group_offset(59, dims) == 60);
            CHECK(tile_group_offset(60, dims) == 0);
            CHECK(tile_group_offset(61, dims) == 0);
            CHECK(tile_group_offset(62, dims) == 12);
            CHECK(tile_group_offset(63, dims) == 12);
            CHECK(tile_group_offset(64, dims) == 24);
            CHECK(tile_group_offset(65, dims) == 0);
            CHECK(tile_group_offset(66, dims) == 0);
            CHECK(tile_group_offset(67, dims) == 12);
            CHECK(tile_group_offset(68, dims) == 12);
            CHECK(tile_group_offset(69, dims) == 24);
            CHECK(tile_group_offset(70, dims) == 36);
            CHECK(tile_group_offset(71, dims) == 36);
            CHECK(tile_group_offset(72, dims) == 48);
            CHECK(tile_group_offset(73, dims) == 48);
            CHECK(tile_group_offset(74, dims) == 60);
            CHECK(tile_group_offset(75, dims) == 0);

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

    acquire_export int unit_test__writer__write_frame_to_chunks()
    {
        const auto base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

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

            zarr::ArrayConfig array_spec = {
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

    acquire_export int unit_test__downsample_writer_config()
    {
        int retval = 0;
        try {
            const fs::path base_dir = "acquire";

            zarr::ArrayConfig config {
                .image_shape = {
                    .dims = {
                      .channels = 1,
                      .width = 64,
                      .height = 48,
                      .planes = 1,
                    },
                    .strides = {
                      .channels = 1,
                      .width = 1,
                      .height = 64,
                      .planes = 64 * 48
                    },
                    .type = SampleType_u8
                },
                .dimensions = {},
                .data_root = (base_dir / "data" / "root" / "0").string(),
                .compression_params = std::nullopt
            };

            config.dimensions.emplace_back(
              "x", DimensionType_Space, 64, 16, 2); // 4 chunks, 2 shards
            config.dimensions.emplace_back(
              "y", DimensionType_Space, 48, 16, 3); // 3 chunks, 1 shard
            config.dimensions.emplace_back(
              "z", DimensionType_Space, 7, 3, 3); // 3 chunks, 3 shards
            config.dimensions.emplace_back(
              "c", DimensionType_Channel, 2, 1, 1); // 2 chunks, 2 shards
            config.dimensions.emplace_back("t",
                                           DimensionType_Time,
                                           0,
                                           5,
                                           1); // 5 timepoints / chunk, 1 shard

            zarr::ArrayConfig downsampled_config;
            CHECK(zarr::downsample(config, downsampled_config));

            // check dimensions
            CHECK(downsampled_config.dimensions.size() == 5);
            CHECK(downsampled_config.dimensions.at(0).name == "x");
            CHECK(downsampled_config.dimensions.at(0).array_size_px == 32);
            CHECK(downsampled_config.dimensions.at(0).chunk_size_px == 16);
            CHECK(downsampled_config.dimensions.at(0).shard_size_chunks == 2);

            CHECK(downsampled_config.dimensions.at(1).name == "y");
            CHECK(downsampled_config.dimensions.at(1).array_size_px == 24);
            CHECK(downsampled_config.dimensions.at(1).chunk_size_px == 16);
            CHECK(downsampled_config.dimensions.at(1).shard_size_chunks == 2);

            CHECK(downsampled_config.dimensions.at(2).name == "z");
            CHECK(downsampled_config.dimensions.at(2).array_size_px == 4);
            CHECK(downsampled_config.dimensions.at(2).chunk_size_px == 3);
            CHECK(downsampled_config.dimensions.at(2).shard_size_chunks == 2);

            CHECK(downsampled_config.dimensions.at(3).name == "c");
            // we don't downsample channels
            CHECK(downsampled_config.dimensions.at(3).array_size_px == 2);
            CHECK(downsampled_config.dimensions.at(3).chunk_size_px == 1);
            CHECK(downsampled_config.dimensions.at(3).shard_size_chunks == 1);

            CHECK(downsampled_config.dimensions.at(4).name == "t");
            CHECK(downsampled_config.dimensions.at(4).array_size_px == 0);
            CHECK(downsampled_config.dimensions.at(4).chunk_size_px == 5);
            CHECK(downsampled_config.dimensions.at(4).shard_size_chunks == 1);

            // check image shape
            CHECK(downsampled_config.image_shape.dims.channels == 1);
            CHECK(downsampled_config.image_shape.dims.width == 32);
            CHECK(downsampled_config.image_shape.dims.height == 24);
            CHECK(downsampled_config.image_shape.dims.planes == 1);

            CHECK(downsampled_config.image_shape.strides.channels == 1);
            CHECK(downsampled_config.image_shape.strides.width == 1);
            CHECK(downsampled_config.image_shape.strides.height == 32);
            CHECK(downsampled_config.image_shape.strides.planes == 32 * 24);

            // check data root
            CHECK(downsampled_config.data_root ==
                  (base_dir / "data" / "root" / "1").string());

            // check compression params
            CHECK(!downsampled_config.compression_params.has_value());

            // downsample again
            config = std::move(downsampled_config);

            // can't downsample anymore
            CHECK(!zarr::downsample(config, downsampled_config));

            // check dimensions
            CHECK(downsampled_config.dimensions.size() == 5);
            CHECK(downsampled_config.dimensions.at(0).name == "x");
            CHECK(downsampled_config.dimensions.at(0).array_size_px == 16);
            CHECK(downsampled_config.dimensions.at(0).chunk_size_px == 16);
            CHECK(downsampled_config.dimensions.at(0).shard_size_chunks == 1);

            CHECK(downsampled_config.dimensions.at(1).name == "y");
            CHECK(downsampled_config.dimensions.at(1).array_size_px == 12);
            CHECK(downsampled_config.dimensions.at(1).chunk_size_px == 12);
            CHECK(downsampled_config.dimensions.at(1).shard_size_chunks == 1);

            CHECK(downsampled_config.dimensions.at(2).name == "z");
            CHECK(downsampled_config.dimensions.at(2).array_size_px == 2);
            CHECK(downsampled_config.dimensions.at(2).chunk_size_px == 2);
            CHECK(downsampled_config.dimensions.at(2).shard_size_chunks == 1);

            CHECK(downsampled_config.dimensions.at(3).name == "c");
            // we don't downsample channels
            CHECK(downsampled_config.dimensions.at(3).array_size_px == 2);
            CHECK(downsampled_config.dimensions.at(3).chunk_size_px == 1);
            CHECK(downsampled_config.dimensions.at(3).shard_size_chunks == 1);

            CHECK(downsampled_config.dimensions.at(4).name == "t");
            CHECK(downsampled_config.dimensions.at(4).array_size_px == 0);
            CHECK(downsampled_config.dimensions.at(4).chunk_size_px == 5);
            CHECK(downsampled_config.dimensions.at(4).shard_size_chunks == 1);

            // check image shape
            CHECK(downsampled_config.image_shape.dims.channels == 1);
            CHECK(downsampled_config.image_shape.dims.width == 16);
            CHECK(downsampled_config.image_shape.dims.height == 12);
            CHECK(downsampled_config.image_shape.dims.planes == 1);

            CHECK(downsampled_config.image_shape.strides.channels == 1);
            CHECK(downsampled_config.image_shape.strides.width == 1);
            CHECK(downsampled_config.image_shape.strides.height == 16);
            CHECK(downsampled_config.image_shape.strides.planes == 16 * 12);

            // check data root
            CHECK(downsampled_config.data_root ==
                  (base_dir / "data" / "root" / "2").string());

            // check compression params
            CHECK(!downsampled_config.compression_params.has_value());

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return retval;
    }
};
#endif
