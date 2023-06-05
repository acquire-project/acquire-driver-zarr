#include "tiled.frame.hh"

#include <algorithm>
#include <stdexcept>

#include "device/props/components.h"

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
} // ::{anonymous}

namespace acquire::sink::zarr {
FrameROI::FrameROI(const ImageShape& image,
                   const TileShape& tile,
                   uint32_t x,
                   uint32_t y,
                   uint32_t p)
  : row_offset_{ 0 }
  , plane_offset_{ 0 }
  , image_{}
  , shape_{}
{
    size_t x_max = std::ceil((float)(image.dims.width * image.dims.channels) /
                             (float)tile.dims.width);
    EXPECT(x < x_max,
           "ChunkWriter column index given as %lu, but maximum value is %lu",
           x,
           x_max - 1);

    size_t y_max =
      std::ceil((float)image.dims.height / (float)tile.dims.height);
    EXPECT(y < y_max,
           "ChunkWriter row index given as %lu, but maximum value is %lu",
           y,
           y_max - 1);

    size_t p_max =
      std::ceil((float)image.dims.planes / (float)tile.dims.planes);
    EXPECT(p < p_max,
           "ChunkWriter plane index given as %lu, but maximum value is %lu",
           p,
           p_max - 1);

    x_ = x;
    y_ = y;
    p_ = p;
    image_ = image;
    shape_ = tile;
}

uint32_t
FrameROI::x() const
{
    return x_;
}

uint32_t
FrameROI::y() const
{
    return y_;
}

uint32_t
FrameROI::p() const
{
    return p_;
}

uint32_t
FrameROI::col() const
{
    return x_ * shape_.dims.width * image_.dims.channels;
}

uint32_t
FrameROI::row() const
{
    return y_ * shape_.dims.height + row_offset_;
}

uint32_t
FrameROI::plane() const
{
    return p_ * shape_.dims.planes + plane_offset_;
}

uint64_t
FrameROI::offset() const
{
    return col() + row() * image_.strides.height +
           plane() * image_.strides.planes;
}

size_t
FrameROI::bytes_per_row() const
{
    return bytes_of_type(image_.type) * image_.dims.channels *
           shape_.dims.width;
}

size_t
FrameROI::bytes_per_tile() const
{
    return bytes_per_row() * shape_.dims.height * shape_.dims.planes;
}

void
FrameROI::increment_row()
{
    row_offset_ = (row_offset_ + 1) % shape_.dims.height;
    if (0 == row_offset_)
        increment_plane();
}

void
FrameROI::increment_plane()
{
    plane_offset_ += 1;
}

bool
FrameROI::finished() const
{
    return plane_offset_ == shape_.dims.planes;
}

void
FrameROI::reset()
{
    row_offset_ = plane_offset_ = 0;
}

const TileShape&
FrameROI::shape() const
{
    return shape_;
}

bool
operator==(const FrameROI& lhs, const FrameROI& rhs)
{
    return lhs.offset() == rhs.offset();
}

TiledFrame::TiledFrame(VideoFrame* frame, const TileShape& tile_shape)
  : buf_{ nullptr }
  , bytes_of_image_{ 0 }
  , tile_shape_{ tile_shape }
{
    CHECK(frame);
    CHECK(buf_ = new uint8_t[frame->bytes_of_frame - sizeof(*frame)]);
    bytes_of_image_ = frame->bytes_of_frame - sizeof(*frame);
    memcpy(buf_, frame->data, bytes_of_image_);
    memcpy(&image_shape_, &frame->shape, sizeof(image_shape_));
    frame_id_ = frame->frame_id;
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
TiledFrame::next_contiguous_region(FrameROI& idx, uint8_t** region) const
{
    size_t nbytes = 0;

    if (idx.row() >= image_shape_.dims.height ||
        idx.plane() >= image_shape_.dims.planes) {
        *region = nullptr;
    } else {
        // widths are in pixels
        size_t img_width = image_shape_.dims.width;
        size_t tile_width = tile_shape_.dims.width;
        size_t region_width =
          std::min(idx.col() + tile_width, img_width) - idx.col();
        nbytes = region_width * bytes_of_type(image_shape_.type);
        *region = buf_ + idx.offset();
    }

    idx.increment_row();
    return nbytes;
}

std::vector<FrameROI>
make_frame_rois(const ImageShape& image_shape, const TileShape& tile_shape)
{
    std::vector<FrameROI> frame_rois;
    size_t img_px_x = image_shape.dims.channels * image_shape.dims.width;
    size_t x_max = std::ceil((float)img_px_x / (float)tile_shape.dims.width);

    size_t img_px_y = image_shape.dims.height;
    size_t y_max = std::ceil((float)img_px_y / (float)tile_shape.dims.height);

    size_t img_px_p = image_shape.dims.planes;
    size_t p_max = std::ceil((float)img_px_p / (float)tile_shape.dims.planes);

    for (auto p = 0; p < p_max; ++p) {
        for (auto i = 0; i < y_max; ++i) {
            for (auto j = 0; j < x_max; ++j) {
                frame_rois.emplace_back(image_shape, tile_shape, j, i, p);
            }
        }
    }

    return frame_rois;
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
    acquire::sink::zarr::TiledFrame tf(&vf, {});

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