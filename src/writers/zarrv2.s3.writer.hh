#ifndef H_ACQUIRE_ZARR_ZARRV2_S3_WRITER_V0
#define H_ACQUIRE_ZARR_ZARRV2_S3_WRITER_V0

#include "s3.writer.hh"

namespace acquire::sink::zarr {
struct ZarrV2S3Writer final : public S3Writer
{
  public:
    ZarrV2S3Writer() = delete;
    ZarrV2S3Writer(const WriterConfig& writer_config,
                   const S3Config& s3_config,
                   std::shared_ptr<ThreadPool> thread_pool);

    ~ZarrV2S3Writer() noexcept override = default;

  protected:
    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;
    void close_() override;
};
}

#endif // H_ACQUIRE_ZARR_ZARRV2_S3_WRITER_V0
