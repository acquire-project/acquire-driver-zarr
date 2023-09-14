#include "sharding.encoder.hh"

#include <cmath>

#include <iostream>
#include <iomanip>

namespace zarr = acquire::sink::zarr;

static size_t
bytes_of_type(const SampleType type)
{
    if (0 > type || type >= SampleTypeCount) {
        LOGE("Invalid parameter: Expected valid sample type. Got value %d.",
             type);
        return 0;
    }

    size_t table[SampleTypeCount] = { 1, 2, 1, 2, 4, 2, 2, 2 };
    return table[type];
}

static size_t
bytes_of_image(const ImageShape& shape)
{
    return shape.strides.planes * bytes_of_type(shape.type);
}

zarr::ShardingEncoder::ShardingEncoder(const ImageDims& image_dims,
                                       const ImageDims& shard_dims,
                                       const ImageDims& chunk_dims)
  : outer_{ image_dims }
  , middle_{ shard_dims }
  , inner_{ chunk_dims }
  , outer_encoder_{ image_dims, shard_dims }
  , inner_encoder_{ shard_dims, chunk_dims }
  , buf_{ nullptr }
  , buf_size_{ 0 }
{
    CHECK(inner_.cols > 0);
    CHECK(inner_.rows > 0);

    CHECK(inner_ <= middle_);
    CHECK(middle_ <= outer_);

    EXPECT(middle_.cols % inner_.cols == 0,
           "Expected shard width to be a multiple of chunk width.");
    EXPECT(middle_.rows % inner_.rows == 0,
           "Expected shard height to be a multiple of chunk height.");
}

zarr::ShardingEncoder::~ShardingEncoder()
{
    delete[] buf_;
    buf_ = nullptr;
    buf_size_ = 0;
}

size_t
zarr::ShardingEncoder::encode(uint8_t* bytes_out,
                              size_t nbytes_out,
                              uint8_t* bytes_in,
                              size_t nbytes_in) const
{
    CHECK(bytes_in);
    CHECK(bytes_out);

    CHECK(nbytes_in > 0);

    const auto npx = outer_.cols * outer_.rows;
    EXPECT(nbytes_in % npx == 0,
           "Expected input buffer to be a multiple of the frame size.");

    const auto bytes_of_type = nbytes_in / npx;

    const auto row_ratio = (float)outer_.rows / (float)middle_.rows;
    const auto col_ratio = (float)outer_.cols / (float)middle_.cols;

    const auto shard_rows = (uint32_t)std::ceil(row_ratio);
    const auto shard_cols = (uint32_t)std::ceil(col_ratio);

    const auto n_shards = shard_rows * shard_cols;
    const auto px_per_shard = middle_.rows * middle_.cols;
    const auto bytes_per_shard = px_per_shard * bytes_of_type;
    const auto bytes_of_sharded_frame = n_shards * bytes_per_shard;

    EXPECT(nbytes_out >= bytes_of_sharded_frame,
           "Expected output buffer to be at least %d bytes. Got %d.",
           bytes_of_sharded_frame,
           nbytes_out);

    if (!buf_) {
        buf_ = new uint8_t[bytes_of_sharded_frame];
        buf_size_ = bytes_of_sharded_frame;
    } else if (buf_size_ < bytes_of_sharded_frame) {
        realloc(buf_, bytes_of_sharded_frame);
    }

    auto b = outer_encoder_.encode(buf_, buf_size_, bytes_in, nbytes_in);

    EXPECT(b == bytes_of_sharded_frame,
           "Expected shard encoder to produce %d bytes. Got %d.",
           bytes_of_sharded_frame,
           b);

    size_t bytes_written = 0;
    for (auto i = 0; i < n_shards; ++i) {
        b = inner_encoder_.encode(bytes_out + bytes_written,
                                  bytes_per_shard,
                                  buf_ + bytes_written,
                                  bytes_per_shard);

        EXPECT(b == bytes_per_shard,
               "Expected chunk encoder to produce %d bytes. Got %d.",
               bytes_per_shard,
               b);

        bytes_written += bytes_per_shard;
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
unit_test__sharding_encoder_encode()
{
    std::vector<uint32_t> frame_in(256);
    for (auto i = 0; i < 256; ++i)
        frame_in.at(i) = i;

    std::vector<uint32_t> frame_out(18 * 18, (uint32_t)-1);

    zarr::ImageDims frame_dims{ 16, 16 };
    zarr::ImageDims shard_dims{ 9, 9 };
    zarr::ImageDims tile_dims{ 3, 3 };

    for (auto i = 0; i < 256; ++i) {
        std::cout << std::setw(4) << frame_in.at(i);
        if (i % 16 == 15)
            std::cout << std::endl;
    }

    zarr::ShardingEncoder encoder{ frame_dims, shard_dims, tile_dims };
    auto nbytes_out = encoder.encode((uint8_t*)frame_out.data(),
                                     frame_out.size() * sizeof(uint32_t),
                                     (uint8_t*)frame_in.data(),
                                     frame_in.size() * sizeof(uint32_t));
    LOG("%d", nbytes_out);

    std::cout << "*****************" << std::endl;

    for (auto i = 0; i < 4; ++i) {
        for (auto j = 0; j < 9; ++j) {
            for (auto k = 0; k < 9; ++k) {
                std::cout << std::setw(4) << frame_out[i * 9 * 9 + j * 9 + k];
            }
            std::cout << std::endl;
        }

        std::cout << std::endl << std::endl;
    }

    //        try {
    //            auto counter = 0;
    //            for (auto i = 0; i < 4; ++i) {
    //                const auto frame_row = 5 * i;
    //                for (auto j = 0; j < 4; ++j) {
    //                    const auto frame_col = 5 * j;
    //                    const auto frame_offset = 16 * frame_row +
    //                    frame_col;
    //
    //                    for (auto tile_row = 0; tile_row < 5; ++tile_row)
    //                    {
    //                        for (auto tile_col = 0; tile_col < 5;
    //                        ++tile_col) {
    //                            const auto tile_offset =
    //                              tile_row * 16 + tile_col + frame_offset;
    //                            if (tile_row + frame_row < 16 &&
    //                                tile_col + frame_col < 16) {
    //                                EXPECT(frame_in.at(tile_offset) ==
    //                                         frame_out.at(counter),
    //                                       "Expected %d==%d, but got
    //                                       %d!=%d",
    //                                       frame_in.at(tile_offset),
    //                                       frame_out.at(counter - 1),
    //                                       frame_in.at(tile_offset),
    //                                       frame_out.at(counter - 1));
    //                            } else {
    //                                CHECK(frame_out.at(counter) == 0);
    //                            }
    //                            ++counter;
    //                        }
    //                    }
    //                }
    //            }
    //        } catch (...) {
    //            return 0;
    //        }

    return 1;
}

#endif
