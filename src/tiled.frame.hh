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
};

class TiledFrame
{
  public:
    TiledFrame() = delete;
    TiledFrame(const VideoFrame* frame,
               const ImageShape&,
               const TileShape& tile_shape);
    TiledFrame(uint8_t* const data,
               uint64_t frame_id,
               size_t layer,
               const ImageShape& image_shape,
               const TileShape& tile_shape);
    TiledFrame(const TiledFrame&) = delete;
    ~TiledFrame();

    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t bytes_of_image() const;

    uint64_t frame_id() const;
    size_t layer() const;
    uint8_t* data() const;

    [[nodiscard]] size_t copy_tile(uint8_t** tile,
                                   uint32_t tile_col,
                                   uint32_t tile_row,
                                   uint32_t tile_plane) const;

  private:
    uint8_t* buf_;

    size_t bytes_of_image_;
    uint64_t frame_id_;
    size_t layer_;

    ImageShape image_shape_;
    TileShape tile_shape_;

    [[nodiscard]] size_t get_contiguous_region(uint8_t** region,
                                               size_t frame_col,
                                               size_t frame_row,
                                               size_t frame_plane,
                                               size_t frame_offset) const;
};
} // acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_TILED_FRAME_V0
