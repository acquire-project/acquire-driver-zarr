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
struct Multiscale
{
    ImageShape image;
    TileShape tile;
};

struct FrameScaler final
{
  public:
    FrameScaler() = delete;
    FrameScaler(const ImageShape& image_shape,
                const TileShape& tile_shape,
                int16_t max_layer,
                uint8_t downscale);
    FrameScaler(const FrameScaler&) = delete;
    ~FrameScaler() = default;

    int16_t max_layer() const noexcept;
    uint8_t downscale() const noexcept;

    [[nodiscard]] bool scale_frame(
      std::shared_ptr<TiledFrame> frame,
      std::function<void(std::shared_ptr<TiledFrame>)> callback) const;

  private:
    const ImageShape& image_shape_;
    const TileShape& tile_shape_;

    const int16_t max_layer_;
    const uint8_t downscale_;

    mutable std::mutex mutex_;
};

std::vector<Multiscale>
get_tile_shapes(const ImageShape& base_image_shape,
                const TileShape& base_tile_shape,
                int16_t max_layer,
                uint8_t downscale);
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_FRAME_SCALER_V0
