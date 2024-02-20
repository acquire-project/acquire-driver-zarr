#ifndef H_ACQUIRE_ZARR_V2_WRITER_V0
#define H_ACQUIRE_ZARR_V2_WRITER_V0

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
struct ZarrV2Writer final : public Writer
{
  public:
    ZarrV2Writer() = delete;
    ZarrV2Writer(const ArraySpec& array_spec,
                 std::shared_ptr<common::ThreadPool> thread_pool);

    ~ZarrV2Writer() override = default;

  private:
    void flush_impl_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V2_WRITER_V0
