#include "chunking.encoder.hh"

#include <cmath>

namespace zarr = acquire::sink::zarr;

zarr::ChunkingEncoder::ChunkingEncoder(const ImageDims& frame_dims,
                                       const ImageDims& tile_dims)
  : outer_{ frame_dims }
  , inner_{ tile_dims }
{
    CHECK(inner_.cols > 0);
    CHECK(inner_.rows > 0);

    CHECK(inner_ <= outer_);
}

size_t
zarr::ChunkingEncoder::encode(uint8_t* bytes_out,
                              size_t nbytes_out,
                              const uint8_t* bytes_in,
                              size_t nbytes_in) const
{
    CHECK(bytes_out);
    CHECK(bytes_in);

    CHECK(nbytes_out >= nbytes_in);

    const auto npx = outer_.cols * outer_.rows;
    EXPECT(nbytes_in % npx == 0,
           "Expected input buffer to be a multiple of the frame size.");

    std::fill(bytes_out, bytes_out + nbytes_out, 0);

    const auto bytes_of_type = nbytes_in / npx;

    const auto row_ratio = (float)outer_.rows / (float)inner_.rows;
    const auto col_ratio = (float)outer_.cols / (float)inner_.cols;

    const auto tile_rows = (uint32_t)std::ceil(row_ratio);
    const auto tile_cols = (uint32_t)std::ceil(col_ratio);

    const auto expected_bytes_out =
      tile_rows * tile_cols * inner_.rows * inner_.cols * bytes_of_type;
    EXPECT(nbytes_out >= expected_bytes_out,
           "Expected output buffer to be at least %d bytes. Got %d.",
           expected_bytes_out,
           nbytes_out);

    size_t bytes_written = 0;
    for (auto i = 0; i < tile_rows; ++i) {
        for (auto j = 0; j < tile_cols; ++j) {
            for (auto k = 0; k < inner_.rows; ++k) {
                const auto frame_row = i * inner_.rows + k;
                if (frame_row < outer_.rows) {
                    const auto frame_col = j * inner_.cols;

                    const auto bytes_in_offset =
                      bytes_of_type * (frame_row * outer_.cols + frame_col);

                    const auto region_width =
                      std::min(frame_col + inner_.cols, outer_.cols) -
                      frame_col;

                    const auto nbytes = region_width * bytes_of_type;

                    std::memcpy(bytes_out + bytes_written,
                                bytes_in + bytes_in_offset,
                                nbytes);
                }
                bytes_written += inner_.cols * bytes_of_type;
            }
        }
    }

    return bytes_written;
}

#ifndef NO_UNIT_TESTS

#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

extern "C" acquire_export int
unit_test__chunking_encoder_encode()
{
    std::vector<uint32_t> frame_in(256);
    for (auto i = 0; i < 256; ++i)
        frame_in[i] = i;

    std::vector<uint32_t> frame_out(400, (uint32_t)-1);

    zarr::ImageDims frame_dims{ 16, 16 };
    zarr::ImageDims tile_dims{ 5, 5 };

    zarr::ChunkingEncoder encoder{ frame_dims, tile_dims };

    try {
        CHECK(20 * 20 * 4 ==
              encoder.encode((uint8_t*)frame_out.data(),
                             frame_out.size() * sizeof(uint32_t),
                             (uint8_t*)frame_in.data(),
                             frame_in.size() * sizeof(uint32_t)));

        auto counter = 0;
        for (auto i = 0; i < 4; ++i) {
            const auto frame_row = 5 * i;
            for (auto j = 0; j < 4; ++j) {
                const auto frame_col = 5 * j;
                const auto frame_offset = 16 * frame_row + frame_col;

                for (auto tile_row = 0; tile_row < 5; ++tile_row) {
                    for (auto tile_col = 0; tile_col < 5; ++tile_col) {
                        const auto tile_offset =
                          tile_row * 16 + tile_col + frame_offset;
                        if (tile_row + frame_row < 16 &&
                            tile_col + frame_col < 16) {
                            EXPECT(frame_in.at(tile_offset) ==
                                     frame_out.at(counter),
                                   "Expected %d==%d, but got %d!=%d",
                                   frame_in.at(tile_offset),
                                   frame_out.at(counter - 1),
                                   frame_in.at(tile_offset),
                                   frame_out.at(counter - 1));
                        } else {
                            CHECK(frame_out.at(counter) == 0);
                        }
                        ++counter;
                    }
                }
            }
        }
    } catch (...) {
        return 0;
    }

    return 1;
}

#endif
