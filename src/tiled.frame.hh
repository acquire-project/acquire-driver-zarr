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
    uint32_t width, height, planes;
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

    size_t bytes_of_image() const;

    uint64_t frame_id() const;
    size_t layer() const;
    uint8_t* data() const;

    /// @brief Copy the tile indexed by @p tile_col, @p tile_row, and
    ///        @p tile_plane into the buffer at @p tile.
    /// @param tile[out] Buffer to copy tile into.
    /// @param bytes_of_tile[in] Size of @p tile.
    /// @param tile_col[in] The column index, in tile space, of the tile.
    /// @param tile_row[in] The row index, in tile space, of the tile.
    /// @param tile_plane[in] The plane index, in tile space, of the tile.
    /// @return The number of bytes written to @p tile. Should be exactly the
    ///         number of bytes in a tile.
    [[nodiscard]] size_t copy_tile(uint8_t* tile,
                                   size_t bytes_of_tile,
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

    /// @brief Get a pointer to the contiguous region determined by
    ///        @p frame_col, @p frame_row, and @p frame_plane, as well as the
    ///        number of bytes
    /// @param region[out] Pointer to pointer to contiguous region.
    /// @param frame_col[in] The column index, in the frame, of the region.
    /// @param frame_row[in] The row index, in the frame, of the region.
    /// @param frame_plane[in] The plane index, in the frame, of the region.
    /// @return The number of bytes pointed to by @p *region.
    [[nodiscard]] size_t get_contiguous_region(uint8_t** region,
                                               size_t frame_col,
                                               size_t frame_row,
                                               size_t frame_plane) const;
};
} // acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_TILED_FRAME_V0
