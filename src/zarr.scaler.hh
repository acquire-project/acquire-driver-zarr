#ifndef H_ACQUIRE_ZARR_SCALER_V0
#define H_ACQUIRE_ZARR_SCALER_V0

#ifdef __cplusplus

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_set>

#include "prelude.h"
#include "tiled.frame.hh"

namespace acquire::sink::zarr {
struct Scaler final
{
  public:
    Scaler() = delete;
    Scaler(const ImageShape& image_shape,
           const TileShape& tile_shape,
           int16_t max_layer,
           uint8_t downscale);
    ~Scaler();

    void push_frame(TiledFrame* frame);
    [[nodiscard]] bool has_frame(uint64_t frame_id) const;
    [[nodiscard]] size_t active_frames() const;
    [[nodiscard]] const TiledFrame* pop_frame_and_make_current();
    void release_current_frame();

    [[nodiscard]] const ImageShape& image_shape() const noexcept;
    [[nodiscard]] const TileShape& tile_shape() const noexcept;

    [[nodiscard]] int16_t max_layer() const noexcept;
    [[nodiscard]] uint8_t downscale() const noexcept;

    [[nodiscard]] std::mutex& mutex() noexcept;

  private:
    const ImageShape& image_shape_;
    const TileShape& tile_shape_;

    const int16_t max_layer_;
    const uint8_t downscale_;

    std::queue<TiledFrame*> frame_ptrs_;
    std::unordered_set<uint64_t> frame_ids_;
    std::optional<uint64_t> current_frame_id_;

    std::mutex mutex_;
};

struct ScalerContext final
{
    Scaler* scaler;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop;
    std::function<void(TiledFrame*)> callback;
};

void
scale_thread(ScalerContext* context);

} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_ZARR_SCALER_V0
