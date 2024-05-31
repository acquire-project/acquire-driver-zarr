#ifndef H_ACQUIRE_ZARR_ZARRV3_FILE_WRITER_V0
#define H_ACQUIRE_ZARR_ZARRV3_FILE_WRITER_V0

#include "file.writer.hh"

namespace acquire::sink::zarr {
struct ZarrV3FileWriter final : public FileWriter
{
    ZarrV3FileWriter(const WriterConfig& config,
                     std::shared_ptr<common::ThreadPool> thread_pool);

    ~ZarrV3FileWriter() noexcept override = default;

  protected:
    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    size_t frames_per_shard_;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_ZARRV3_FILE_WRITER_V0
