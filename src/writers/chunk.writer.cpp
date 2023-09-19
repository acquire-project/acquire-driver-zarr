#include "chunk.writer.hh"

#include <cmath>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root)
  , frame_dims_{ frame_dims }
  , tile_dims_{ tile_dims }
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
        const auto bytes_of_type = common::bytes_of_type(pixel_type_);
        const auto bytes_per_tile =
          tile_dims_.cols * tile_dims_.rows * bytes_of_type;
        const auto n_tiles = tile_rows_ * tile_cols_;
        const auto bytes_of_tiled_frame = n_tiles * bytes_per_tile;

        if (buffers_.empty()) {
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
        LOGE("Failed to write frame: %s", exc.what());
    } catch (...) {
        LOGE("Failed to write frame (unknown)");
    }

    return false;
}

void
zarr::ChunkWriter::make_buffers_() noexcept
{
    buffers_.resize(tile_rows_ * tile_cols_);

    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;
    for (auto& buf : buffers_) {
        buf.resize(frames_per_chunk_ * bytes_per_tile);
        std::fill(buf.begin(), buf.end(), 0);
    }
}

size_t
zarr::ChunkWriter::write_bytes_(const uint8_t* buf, size_t buf_size) noexcept
{
    // FIXME (aliddell): assumes a single frame is written
    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;
    const auto frames_this_chunk = frames_written_ % frames_per_chunk_;

    size_t bytes_written = 0;

    for (auto i = 0; i < tile_rows_; ++i) {
        for (auto j = 0; j < tile_cols_; ++j) {
            size_t offset = bytes_per_tile * frames_this_chunk;

            uint8_t* bytes_out = buffers_.at(i * tile_cols_ + j).data();
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
    if (frames_written_ == 0) {
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
    const auto bytes_per_chunk = bytes_to_flush_ / (tile_rows_ * tile_cols_);

    // create chunk files if necessary
    if (files_.empty()) {
        make_files_();
    }

    // write out to each chunk
    {
        std::scoped_lock lock(mutex_);

        for (auto i = 0; i < files_.size(); ++i) {
            jobs_.emplace(
              buffers_.at(i).data(), bytes_per_chunk, &files_.at(i), 0);
        }
    }

    // wait for all writers to finish
    while (!jobs_.empty()) {
        std::this_thread::sleep_for(2ms);
    }

    for (auto& buf: buffers_) {
        std::fill(buf.begin(), buf.end(), 0);
    }
    bytes_to_flush_ = 0;
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
        zarr::ChunkWriter writer{ frame_dims, tile_dims, 8, data_dir.string() };

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
