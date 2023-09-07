#ifndef H_ACQUIRE_STORAGE_ZARR_V3_V0
#define H_ACQUIRE_STORAGE_ZARR_V3_V0

#include "../zarr.hh"

#ifndef __cplusplus
#error "This header requires C++20"
#endif

namespace acquire::sink::zarr {
struct ZarrV3 final : public Zarr
{
  public:
    ZarrV3() = default;
    explicit ZarrV3(CompressionParams&& compression_params);
    ~ZarrV3() override = default;

  private:
    /// Metadata
    void write_array_metadata_(size_t level,
                               const ImageShape& image_shape,
                               const TileShape& tile_shape) const override;
    void write_external_metadata_() const override;
    void write_base_metadata_() const override;
    void write_group_metadata_() const override;

    /// Filesystem
    std::string get_data_directory_() const override;
    std::string get_chunk_dir_prefix_() const override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_V3_V0
