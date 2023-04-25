#ifndef H_ACQUIRE_STORAGE_ZARR_TILED_FRAME_V0
#define H_ACQUIRE_STORAGE_ZARR_TILED_FRAME_V0

#include <cassert>
#include <cmath>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "device/props/components.h"

#include "prelude.h"

namespace acquire::sink::zarr {
struct TileShape
{
    struct tile_shape_dims_s
    {
        uint32_t width, height, planes;
    } dims;
    struct tile_shape_frame_channels_s
    {
        uint8_t nchannels;
        uint32_t frames_per_channel;
    } frame_channels;
};

struct FrameROI
{
  public:
    FrameROI() = delete;
    FrameROI(const ImageShape& image,
             const TileShape& tile,
             uint32_t x,
             uint32_t y,
             uint32_t p);

    [[nodiscard]] uint32_t x() const;
    [[nodiscard]] uint32_t y() const;
    [[nodiscard]] uint32_t p() const;

    [[nodiscard]] uint32_t col() const;
    [[nodiscard]] uint32_t row() const;
    [[nodiscard]] uint32_t plane() const;
    [[nodiscard]] uint64_t offset() const;
    [[nodiscard]] size_t bytes_per_row() const;
    [[nodiscard]] size_t bytes_per_tile() const;

    void increment_row();
    void increment_plane();
    [[nodiscard]] bool finished() const;
    void reset();

    [[nodiscard]] const ImageShape& image() const;
    [[nodiscard]] const TileShape& shape() const;

  private:
    ImageShape image_;
    TileShape shape_;

    uint32_t row_offset_;
    uint32_t plane_offset_;
    uint32_t x_, y_, p_;
};

class TiledFrame
{
  public:
    TiledFrame() = delete;
    TiledFrame(VideoFrame* frame, const TileShape& tile_shape);
    ~TiledFrame() = default;

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t bytes_of_image() const;

    [[nodiscard]] uint64_t frame_id() const;
    [[nodiscard]] const ImageShape& image_shape() const;
    [[nodiscard]] uint8_t* data() const;

    [[nodiscard]] size_t next_contiguous_region(FrameROI& idx,
                                                uint8_t** region) const;

  private:
    VideoFrame* frame_;
    TileShape tile_shape_;
};

std::vector<FrameROI>
make_frame_rois(const ImageShape& image_shape, const TileShape& tile_shape);
} // acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_TILED_FRAME_V0
