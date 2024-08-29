#pragma once

#include "array.writer.hh"

namespace zarr {
struct ZarrV3ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter(
      const ArrayWriterConfig& array_spec,
      std::shared_ptr<ThreadPool> thread_pool,
      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    ~ZarrV3ArrayWriter() override = default;

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr
