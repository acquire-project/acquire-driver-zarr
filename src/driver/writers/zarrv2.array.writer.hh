#pragma once

#include "array.writer.hh"

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
struct ZarrV2ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV2ArrayWriter() = delete;
    ZarrV2ArrayWriter(
      const ArrayWriterConfig& config,
      std::shared_ptr<common::ThreadPool> thread_pool,
      std::shared_ptr<common::S3ConnectionPool> connection_pool);

    ~ZarrV2ArrayWriter() override = default;

  private:
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr
