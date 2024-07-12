#ifndef H_ACQUIRE_ZARR_V3_WRITER_V0
#define H_ACQUIRE_ZARR_V3_WRITER_V0

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
    ZarrV3Writer(const WriterConfig& array_spec,
                 std::shared_ptr<common::ThreadPool> thread_pool,
                 std::shared_ptr<common::S3ConnectionPool> connection_pool);

    ~ZarrV3Writer() override = default;

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V3_WRITER_V0
