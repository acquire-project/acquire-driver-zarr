#ifndef H_ACQUIRE_STORAGE_ZARR_V3_V0
#define H_ACQUIRE_STORAGE_ZARR_V3_V0

#include "zarr.hh"

namespace acquire::sink::zarr {
struct ZarrV3 final : public Zarr
{
  public:
    ZarrV3() = default;
    explicit ZarrV3(BloscCompressionParams&& compression_params);
    ~ZarrV3() override = default;

    /// Storage interface
    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void get_meta(StoragePropertyMetadata* meta) const override;
    void reserve_image_shape(const ImageShape* shape) override;

  private:
    using ShardSize = StorageProperties::storage_properties_shard_size_s;
    using ShardingMeta =
      StoragePropertyMetadata::storage_property_metadata_sharding_s;

    std::vector<ShardSize> shard_size_chunks_;

    /// Setup
    void set_sharding(const ShardSize& size, const ShardingMeta& meta);
    void allocate_writers_() override;

    /// Metadata
    void write_array_metadata_(size_t level) const override;
    void write_external_metadata_() const override;
    void write_base_metadata_() const override;
    void write_group_metadata_() const override;

    /// Filesystem
    fs::path get_data_directory_() const override;
};
} // namespace acquire::sink::zarr
#endif // H_ACQUIRE_STORAGE_ZARR_V3_V0
