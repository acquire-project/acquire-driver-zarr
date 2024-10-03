#pragma once

#include "array.writer.hh"

namespace zarr {
struct ZarrV3ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter(ArrayWriterConfig&& config,
                      std::shared_ptr<ThreadPool> thread_pool);
    ZarrV3ArrayWriter(
      ArrayWriterConfig&& config,
      std::shared_ptr<ThreadPool> thread_pool,
      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    ~ZarrV3ArrayWriter() override;

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    ZarrVersion version_() const override;
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
