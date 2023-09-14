#ifndef H_ACQUIRE_STORAGE_ZARR_WRITER_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "device/props/components.h"

template<typename WriterT>
concept Writer = requires(WriterT w, const VideoFrame* frame) {
    // clang-format off
    { w.write(frame) } -> std::convertible_to<bool>;
    // clang-format on
};

#endif // H_ACQUIRE_STORAGE_ZARR_WRITER_V0
