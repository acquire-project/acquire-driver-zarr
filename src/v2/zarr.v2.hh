#ifndef H_ACQUIRE_STORAGE_ZARR_V2_V0
#define H_ACQUIRE_STORAGE_ZARR_V2_V0

#include "../zarr.hh"

#ifndef __cplusplus
#error "This header requires C++20"
#endif

namespace acquire::sink::zarr {
struct ZarrV2 final : public Zarr
{
  public:
    ZarrV2() = default;
    explicit ZarrV2(CompressionParams&& compression_params);
    ~ZarrV2() override = default;

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
    std::string get_chunk_dir_prefix_() const override; // TODO: remove
};
}

#endif // H_ACQUIRE_STORAGE_ZARR_V2_V0
