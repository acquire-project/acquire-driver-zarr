#ifndef H_ACQUIRE_ZARR_ZARRV3_S3_WRITER_V0
#define H_ACQUIRE_ZARR_ZARRV3_S3_WRITER_V0

#include "s3.writer.hh"

namespace acquire::sink::zarr {
struct ZarrV3S3Writer final : public S3Writer
{
  public:
    ZarrV3S3Writer() = delete;
    ZarrV3S3Writer(const WriterConfig& writer_config,
                   const S3Config& s3_config,
                   std::shared_ptr<ThreadPool> thread_pool);
    ~ZarrV3S3Writer() noexcept override = default;

  protected:
    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;
    void close_() override;

  private:
    std::vector<std::vector<uint64_t>> shard_tables_;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_ZARRV3_S3_WRITER_V0
