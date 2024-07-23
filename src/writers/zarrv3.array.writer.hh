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
struct ZarrV3ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter() = delete;
    ZarrV3ArrayWriter(
      const WriterConfig& array_spec,
      std::shared_ptr<common::ThreadPool> thread_pool,
      std::shared_ptr<common::S3ConnectionPool> connection_pool);

    ~ZarrV3ArrayWriter() override = default;

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr

