#include "chunk.writer.hh"

#include <cmath>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ChunkWriter::ChunkWriter(const ImageDims& frame_dims,
                               const ImageDims& tile_dims,
                               uint32_t frames_per_chunk,
                               const std::string& data_root)
  : Writer(frame_dims, tile_dims, frames_per_chunk, data_root)
  , chunking_encoder_{ frame_dims, tile_dims }
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

        write_bytes_(buf_.data(), bytes_encoded);

        // create chunk files if necessary
        if (files_.empty()) {
            make_files_();
        }

        // rollover if necessary
        const auto frames_this_chunk = frames_written_ % frames_per_chunk_;
        if (frames_written_ > 0 && frames_this_chunk == 0) {
            rollover_();
        }

        ++frames_written_;
        return true;
    } catch (const std::exception& exc) {
        LOGE("Failed to write frame: %s", exc.what());
    } catch (...) {
        LOGE("Failed to write frame (unknown)");
    }

    return false;
}

void
zarr::ChunkWriter::write_bytes_(const uint8_t* buf, size_t buf_size) noexcept
{

}

void
zarr::ChunkWriter::flush_() noexcept
{
    using namespace std::chrono_literals;

    // write out to each chunk
    {
        std::scoped_lock lock(mutex_);

        for (auto i = 0; i < files_.size(); ++i) {
            jobs_.emplace(buf_.data() + i * bytes_per_tile,
                          bytes_per_tile,
                          &files_.at(i),
                          bytes_per_tile * frames_this_chunk);
        }
    }

    // wait for all writers to finish
    while (!jobs_.empty()) {
        std::this_thread::sleep_for(2ms);
    }
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
