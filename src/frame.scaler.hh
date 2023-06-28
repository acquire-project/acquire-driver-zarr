#ifndef H_ACQUIRE_ZARR_FRAME_SCALER_V0
#define H_ACQUIRE_ZARR_FRAME_SCALER_V0

#ifdef __cplusplus

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_set>

#include "prelude.h"
#include "tiled.frame.hh"
#include "chunk.writer.hh"

namespace acquire::sink::zarr {
class Zarr;

struct Multiscale
{
    ImageShape image;
    TileShape tile;
    Multiscale(const ImageShape& image_shape,
               const TileShape& tile_shape);
};

struct FrameScaler final
{
  public:
    FrameScaler() = delete;
    FrameScaler(Zarr* zarr,
                const ImageShape& image_shape,
                const TileShape& tile_shape,
                int16_t max_layer);
    FrameScaler(const FrameScaler&) = delete;
    ~FrameScaler() = default;

    int16_t max_layer() const noexcept;

    [[nodiscard]] bool scale_frame(std::shared_ptr<TiledFrame> frame) const;

  private:
    Zarr* zarr_; // non-owning

    const ImageShape& image_shape_;
    const TileShape& tile_shape_;

    const int16_t max_layer_;

    mutable std::mutex mutex_;
};

std::vector<Multiscale>
get_tile_shapes(const ImageShape& base_image_shape,
                const TileShape& base_tile_shape,
                int16_t max_layer);
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_FRAME_SCALER_V0
