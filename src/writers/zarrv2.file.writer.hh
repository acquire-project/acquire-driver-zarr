#ifndef H_ACQUIRE_ZARR_ZARRV2_FILE_WRITER_V0
#define H_ACQUIRE_ZARR_ZARRV2_FILE_WRITER_V0

#include "zarrv2.writer.hh"
#include "file.writer.hh"

namespace acquire::sink::zarr {
struct ZarrV2FileWriter final : public ZarrV2Writer, public FileWriter
{
    ZarrV2FileWriter(const WriterConfig& config,
                     std::shared_ptr<common::ThreadPool> thread_pool);

    ~ZarrV2FileWriter() noexcept override = default;

  protected:
    [[nodiscard]] bool flush_impl_() override;
    bool should_rollover_() const override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_ZARRV2_FILE_WRITER_V0
