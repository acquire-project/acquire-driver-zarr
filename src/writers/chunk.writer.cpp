#include "chunk.writer.hh"

#include <cmath>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root)
{
    // pare down the number of threads if we have too many
    while (threads_.size() > tiles_per_frame_()) {
        threads_.pop_back();
    }

    // spin up threads
    for (auto& ctx : threads_) {
        ctx.should_stop = false;
        ctx.thread =
          std::thread([this, capture0 = &ctx] { worker_thread_(capture0); });
    }
}

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root,
                               const BloscCompressionParams& compression_params)
  : Writer(frame_dims,
           tile_dims,
           frames_per_chunk,
           data_root,
           compression_params)
{
    // pare down the number of threads if we have too many
    while (threads_.size() > tiles_per_frame_()) {
        threads_.pop_back();
    }

    // spin up threads
    for (auto& ctx : threads_) {
        ctx.should_stop = false;
        ctx.thread =
          std::thread([this, capture0 = &ctx] { worker_thread_(capture0); });
    }
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
        LOGE("Failed to write frame: %s", exc.what());
    } catch (...) {
        LOGE("Failed to write frame (unknown)");
    }

    return false;
}

void
zarr::ChunkWriter::make_buffers_() noexcept
{
    chunk_buffers_.resize(tiles_per_frame_());

    const auto bytes_of_type = common::bytes_of_type(pixel_type_);
    const auto bytes_per_tile =
      tile_dims_.cols * tile_dims_.rows * bytes_of_type;

    for (auto& buf : chunk_buffers_) {
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
        LOGE("Failed to flush.");
        return;
    }

    // compress buffers and write out
    auto buf_sizes = compress_buffers_();
    {
        std::scoped_lock lock(mutex_);
        for (auto i = 0; i < files_.size(); ++i) {
            auto& buf = chunk_buffers_.at(i);
            jobs_.emplace(buf.data(), buf_sizes.at(i), &files_.at(i), 0);
        }
    }

    // wait for all writers to finish
    while (!jobs_.empty()) {
        std::this_thread::sleep_for(2ms);
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
    for (auto y = 0; y < tiles_per_frame_y_; ++y) {
        for (auto x = 0; x < tiles_per_frame_x_; ++x) {
            const auto filename = data_root_ / std::to_string(current_chunk_) /
                                  "0" / std::to_string(y) / std::to_string(x);
            fs::create_directories(filename.parent_path());
            files_.emplace_back();
            if (!file_create(&files_.back(),
                             filename.string().c_str(),
                             filename.string().size())) {
                LOGE("Failed to create file '%s'", filename.string().c_str());
                return false;
            }
        }
    }
    return true;
}

#ifndef NO_UNIT_TESTS

#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

#include <fstream>
#include <iostream>

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

        for (auto i = 0; i < 100; ++i)
            CHECK(writer.write((VideoFrame*)frame_ptr));
        delete frame_ptr;

        writer.finalize();

        // test file contents
        std::vector<uint16_t> buf(8 * 8);

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "0" / "0"));
        std::ifstream fh(data_dir / "0" / "0" / "0" / "0", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            EXPECT(buf.at(i) == i,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i,
                   i,
                   buf.at(i));
            EXPECT(buf.at(i + 8) == i + 16,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 8,
                   i + 16,
                   buf.at(i + 8));
            EXPECT(buf.at(i + 16) == i + 32,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 16,
                   i + 32,
                   buf.at(i + 16));
            EXPECT(buf.at(i + 24) == i + 48,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 24,
                   i + 48,
                   buf.at(i + 24));
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "0" / "1"));
        fh = std::ifstream(data_dir / "0" / "0" / "0" / "1", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            EXPECT(buf.at(i) == i + 8,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i,
                   i + 8,
                   buf.at(i));
            EXPECT(buf.at(i + 8) == i + 24,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 8,
                   i + 24,
                   buf.at(i + 8));
            EXPECT(buf.at(i + 16) == i + 40,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 16,
                   i + 40,
                   buf.at(i + 16));
            EXPECT(buf.at(i + 24) == i + 56,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 24,
                   i + 56,
                   buf.at(i + 24));
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "1" / "0"));
        fh = std::ifstream(data_dir / "0" / "0" / "1" / "0", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            EXPECT(buf.at(i) == i + 128,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i,
                   i + 128,
                   buf.at(i));
            EXPECT(buf.at(i + 8) == i + 144,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 8,
                   i + 144,
                   buf.at(i + 8));
            EXPECT(buf.at(i + 16) == i + 160,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 16,
                   i + 160,
                   buf.at(i + 16));
            EXPECT(buf.at(i + 24) == i + 176,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 24,
                   i + 176,
                   buf.at(i + 24));
        }

        CHECK(fs::is_regular_file(data_dir / "0" / "0" / "1" / "1"));
        fh = std::ifstream(data_dir / "0" / "0" / "1" / "1", std::ios::binary);
        fh.read((char*)buf.data(), buf.size() * 2);
        for (auto i = 0; i < 8; ++i) {
            EXPECT(buf.at(i) == i + 136,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i,
                   i + 136,
                   buf.at(i));
            EXPECT(buf.at(i + 8) == i + 152,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 8,
                   i + 152,
                   buf.at(i + 8));
            EXPECT(buf.at(i + 16) == i + 168,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 16,
                   i + 168,
                   buf.at(i + 16));
            EXPECT(buf.at(i + 24) == i + 184,
                   "Expected buf.at(%d) == %d, but actually %d",
                   i + 24,
                   i + 184,
                   buf.at(i + 24));
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
        const std::string err_msg{ e.what() };
        std::cout << err_msg << std::endl;
        LOGE("Error: %s", e.what());
    } catch (...) {
        LOGE("Unknown error");
    }

    return 0;
}
#endif
