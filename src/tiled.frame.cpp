#include "tiled.frame.hh"

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include "device/props/components.h"

namespace zarr = acquire::sink::zarr;

namespace {
size_t
bytes_of_type(const SampleType& type)
{
    CHECK(type < SampleTypeCount);
    static size_t table[SampleTypeCount]; // = { 1, 2, 1, 2, 4, 2, 2, 2 };
#define XXX(s, b) table[(s)] = (b)
    XXX(SampleType_u8, 1);
    XXX(SampleType_u16, 2);
    XXX(SampleType_i8, 1);
    XXX(SampleType_i16, 2);
    XXX(SampleType_f32, 4);
    XXX(SampleType_u10, 2);
    XXX(SampleType_u12, 2);
    XXX(SampleType_u14, 2);
#undef XXX
    return table[type];
}

size_t
bytes_per_tile(const ImageShape& image, const zarr::TileShape& tile)
{
    return bytes_of_type(image.type) * image.dims.channels * tile.dims.width *
           tile.dims.height * tile.dims.planes;
}
} // ::{anonymous}

namespace acquire::sink::zarr {
TiledFrame::TiledFrame(const VideoFrame* frame,
                       const ImageShape& image_shape,
                       const TileShape& tile_shape)
  : buf_{ nullptr }
  , bytes_of_image_{ 0 }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
{
    CHECK(frame);
    bytes_of_image_ = frame->bytes_of_frame - sizeof(*frame);

    CHECK(frame->data);
    CHECK(buf_ = new uint8_t[bytes_of_image_]);
    memcpy(buf_, frame->data, bytes_of_image_);

    frame_id_ = frame->frame_id;
}

TiledFrame::TiledFrame(uint8_t* const data,
                       size_t bytes_of_image,
                       uint64_t frame_id,
                       const ImageShape& image_shape,
                       const TileShape& tile_shape)
  : buf_{ nullptr }
  , bytes_of_image_{ bytes_of_image }
  , frame_id_{ frame_id }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
{
    CHECK(data);
    CHECK(buf_ = new uint8_t[bytes_of_image]);
    memcpy(buf_, data, bytes_of_image);
}

TiledFrame::~TiledFrame()
{
    delete buf_;
}

size_t
TiledFrame::size() const
{
    return bytes_of_image() / bytes_of_type(image_shape_.type);
}

size_t
TiledFrame::bytes_of_image() const
{
    return bytes_of_image_;
}

uint64_t
TiledFrame::frame_id() const
{
    return frame_id_;
}

uint8_t*
TiledFrame::data() const
{
    return buf_;
}

size_t
TiledFrame::copy_tile(uint8_t** tile,
                      uint32_t tile_col,
                      uint32_t tile_row,
                      uint32_t tile_plane) const
{
    CHECK(tile);
    uint8_t* region = nullptr;

    const size_t bytes_per_row = bytes_of_type(image_shape_.type) *
                                 image_shape_.dims.channels *
                                 tile_shape_.dims.width;
    std::vector<uint8_t> fill(bytes_per_row, 0);

    size_t nbytes_out = 0;

    size_t offset = 0;
    size_t frame_col =
      tile_col * tile_shape_.dims.width * image_shape_.dims.channels;
    for (auto p = 0; p < tile_shape_.dims.planes; ++p) {
        size_t frame_plane = tile_plane * tile_shape_.dims.planes + p;
        for (auto r = 0; r < tile_shape_.dims.height; ++r) {
            size_t frame_row = tile_row * tile_shape_.dims.height + r;
            size_t frame_offset = frame_col +
                                  frame_row * image_shape_.strides.height +
                                  frame_plane * image_shape_.strides.planes;

            size_t nbytes_row = get_contiguous_region(
              &region, frame_col, frame_row, frame_plane, frame_offset);

            // copy frame data into the tile buffer
            if (0 < nbytes_row) {
                CHECK(nullptr != region);
                memcpy(*tile + offset, region, nbytes_row);
            }
            offset += nbytes_row;

            // fill the rest of the row with zeroes
            if (nbytes_row < bytes_per_row) {
                memcpy(*tile + offset, fill.data(), bytes_per_row - nbytes_row);
            }
            offset += bytes_per_row - nbytes_row;

            nbytes_out += bytes_per_row;
        }
    }

    return nbytes_out;
}

size_t
TiledFrame::get_contiguous_region(uint8_t** region,
                                  size_t frame_col,
                                  size_t frame_row,
                                  size_t frame_plane,
                                  size_t frame_offset) const
{
    size_t nbytes = 0;

    if (frame_row >= image_shape_.dims.height ||
        frame_plane >= image_shape_.dims.planes) {
        *region = nullptr;
    } else {
        // widths are in pixels
        size_t img_width = image_shape_.dims.width;
        size_t tile_width = tile_shape_.dims.width;
        size_t region_width =
          std::min(frame_col + tile_width, img_width) - frame_col;
        nbytes = region_width * bytes_of_type(image_shape_.type);
        *region = buf_ + frame_offset;
    }

    return nbytes;
}
} // acquire::sink::zarr

#ifndef NO_UNIT_TESTS
int
unit_test__tiled_frame_size()
{
    VideoFrame vf
    {
        .bytes_of_frame = 2 * 64 * 48 + sizeof(vf),
        .shape = {
            .dims = {
              .channels = 1,
              .width = 64,
              .height = 48,
              .planes = 1,
            },
            .strides = {
              .channels = 1,
              .width = 1,
              .height = 48,
              .planes = 64 * 48
            },
            .type = SampleType_u16,
        },
          .frame_id = 0,
          .hardware_frame_id = 0,
          .timestamps = {
            .hardware = 0,
            .acq_thread = 0,
        },
    };
    acquire::sink::zarr::TiledFrame tf(&vf, {}, {});

    try {
        CHECK(48 * 64 == tf.size());
        CHECK(2 * 48 * 64 == tf.bytes_of_image());
        return 1;
    } catch (const std::exception& e) {
        LOGE("Received std::exception: %s", e.what());
    } catch (...) {
        LOGE("Received exception (unknown)");
    }
    return 0;
}
#endif