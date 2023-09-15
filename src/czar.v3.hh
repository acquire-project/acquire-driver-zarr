#ifndef H_ACQUIRE_STORAGE_CZAR_V3_V0
#define H_ACQUIRE_STORAGE_CZAR_V3_V0

#include "czar.hh"

namespace acquire::sink::zarr {
struct CzarV3 final : public Czar
{
  public:
    CzarV3() = default;
    CzarV3(CompressionParams&& compression_params);
    ~CzarV3() override = default;

    /// StorageInterface
    void get_meta(StoragePropertyMetadata* meta) const override;

  private:
    /// Setup
    void allocate_writers_() override;

    /// Metadata
    void write_array_metadata_(size_t level,
                               const ImageDims& image_shape,
                               const ImageDims& tile_shape) const override;
    void write_external_metadata_() const override;
    void write_base_metadata_() const override;
    void write_group_metadata_() const override;

    /// Filesystem
    std::string get_data_directory_() const override;
    std::string get_chunk_dir_prefix_() const override;
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_STORAGE_CZAR_V3_V0
