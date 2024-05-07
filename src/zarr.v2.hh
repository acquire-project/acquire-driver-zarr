#ifndef H_ACQUIRE_STORAGE_ZARR_V2_V0
#define H_ACQUIRE_STORAGE_ZARR_V2_V0

#include "zarr.hh"

namespace acquire::sink::zarr {
struct ZarrV2 final : public Zarr
{
  public:
    ZarrV2() = default;
    ZarrV2(BloscCompressionParams&& compression_params);
    ~ZarrV2() override = default;

    /// StorageInterface
    void get_meta(StoragePropertyMetadata* meta) const override;

  private:
    /// Setup
    void allocate_writers_() override;

    /// Metadata
    std::vector<std::string> make_metadata_sink_paths_() override;

    // fixed metadata
    void write_base_metadata_() const override;
    void write_external_metadata_() const override;

    // mutable metadata, changes on flush
    void write_group_metadata_() const override;
    void write_array_metadata_(size_t level) const override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V2_V0
