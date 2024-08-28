#pragma once

#include "array.writer.hh"

namespace zarr {
class ZarrV2ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV2ArrayWriter() = delete;
    ZarrV2ArrayWriter(
      const ArrayWriterConfig& config,
      std::shared_ptr<ThreadPool> thread_pool,
      std::shared_ptr<S3ConnectionPool> connection_pool);

    ~ZarrV2ArrayWriter() override = default;

  private:
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr
