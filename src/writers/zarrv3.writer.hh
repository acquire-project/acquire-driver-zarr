#ifndef H_ACQUIRE_ZARR_V3_WRITER_V0
#define H_ACQUIRE_ZARR_V3_WRITER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "writer.hh"

#include "platform.h"
#include "device/props/components.h"

#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct ZarrV3Writer final : public Writer
{
  public:
    ZarrV3Writer() = delete;
    ZarrV3Writer(const ArraySpec& array_spec,
                 std::shared_ptr<common::ThreadPool> thread_pool);

    ~ZarrV3Writer() override = default;

  private:
    bool should_flush_() const noexcept override;
    [[nodiscard]] bool flush_impl_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V3_WRITER_V0
