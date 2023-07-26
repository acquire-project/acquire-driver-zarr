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
    ImageShape image_shape;
    TileShape tile_shape;
    Multiscale(const ImageShape& image_shape, const TileShape& tile_shape);
};

struct FrameScaler final
{
  public:
    FrameScaler() = delete;
    FrameScaler(Zarr* zarr,
                const ImageShape& image_shape,
                const TileShape& tile_shape);
    FrameScaler(const FrameScaler&) = delete;
    ~FrameScaler() = default;

    [[nodiscard]] bool push_frame(std::shared_ptr<TiledFrame> frame);

  private:
    Zarr* zarr_; // non-owning

    std::vector<Multiscale> multiscales_;

    // Accumulate downsampled layers until we have enough to average and write.
    std::unordered_map<int16_t, std::vector<std::shared_ptr<TiledFrame>>>
      accumulators_;

    mutable std::mutex mutex_;

    void downsample_and_accumulate(std::shared_ptr<TiledFrame> frame,
                                   int16_t layer);
};

std::vector<Multiscale>
get_tile_shapes(const ImageShape& base_image_shape,
                const TileShape& base_tile_shape);
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_FRAME_SCALER_V0
