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
    return bytes_of_type(image.type) * image.dims.channels * tile.width *
           tile.height * tile.planes;
}
} // ::{anonymous}

namespace acquire::sink::zarr {
TiledFrame::TiledFrame(const VideoFrame* frame,
                       const ImageShape& image_shape,
                       const TileShape& tile_shape)
  : bytes_of_image_{ 0 }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
{
    CHECK(frame);
    CHECK(frame->data);

    bytes_of_image_ = frame->bytes_of_frame - sizeof(*frame);
    CHECK(bytes_of_image_ > 0);

    buf_.resize(bytes_of_image_);
    memcpy(buf_.data(), frame->data, bytes_of_image_);

    frame_id_ = frame->frame_id;
}

size_t
TiledFrame::copy_tile(uint8_t* tile,
                      size_t bytes_of_tile,
                      uint32_t tile_col,
                      uint32_t tile_row,
                      uint32_t tile_plane) const
{
    CHECK(tile);
    CHECK(bytes_of_tile == bytes_per_tile(image_shape_, tile_shape_));
    memset(tile, 0, bytes_of_tile);

    uint8_t* region = nullptr;

    const size_t bytes_per_row = bytes_of_type(image_shape_.type) *
                                 image_shape_.dims.channels * tile_shape_.width;

    size_t offset = 0;
    uint32_t frame_col =
      tile_col * tile_shape_.width * image_shape_.dims.channels;
    for (auto p = 0; p < tile_shape_.planes; ++p) {
        size_t frame_plane = tile_plane * tile_shape_.planes + p;
        for (auto r = 0; r < tile_shape_.height; ++r) {
            uint32_t frame_row = tile_row * tile_shape_.height + r;

            size_t nbytes_row =
              get_contiguous_region(&region, frame_col, frame_row, frame_plane);

            // copy frame data into the tile buffer
            if (0 < nbytes_row) {
                CHECK(nullptr != region);
                memcpy(tile + offset, region, nbytes_row);
            }

            offset += bytes_per_row;
        }
    }

    return offset;
}

size_t
TiledFrame::get_contiguous_region(uint8_t** region,
                                  size_t frame_col,
                                  size_t frame_row,
                                  size_t frame_plane) const
{
    size_t nbytes = 0;

    auto* data = const_cast<uint8_t*>(buf_.data());

    if (frame_row >= image_shape_.dims.height ||
        frame_plane >= image_shape_.dims.planes) {
        *region = nullptr;
    } else {
        size_t frame_offset =
          bytes_of_type(image_shape_.type) *
          (frame_col + frame_row * image_shape_.strides.height +
           frame_plane * image_shape_.strides.planes);
        // widths are in pixels
        size_t img_width = image_shape_.dims.width;
        size_t tile_width = tile_shape_.width;
        size_t region_width =
          std::min(frame_col + tile_width, img_width) - frame_col;
        nbytes = region_width * bytes_of_type(image_shape_.type);
        *region = data + frame_offset;
    }

    return nbytes;
}
} // acquire::sink::zarr
