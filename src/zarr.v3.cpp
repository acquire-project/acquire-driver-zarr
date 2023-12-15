#include "zarr.v3.hh"
#include "writers/zarrv3.writer.hh"

#include "json.hpp"

#include <cmath>

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_v3_init()
{
    try {
        zarr::BloscCompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::ZarrV3(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

zarr::ZarrV3::ZarrV3(BloscCompressionParams&& compression_params)
  : Zarr(std::move(compression_params))
{
}

void
zarr::ZarrV3::set_sharding(const ShardingProps& props, const ShardingMeta& meta)
{
    // we can't validate and convert until we know the image shape and corrected
    // tile shape (see reserve_image_shape) so we need to store the raw (i.e.,
    // per chunk) values.
    shard_dims_chunks_.clear();
    shard_dims_chunks_.emplace_back(
      std::clamp(
        props.width, (uint32_t)meta.width.low, (uint32_t)meta.width.high),
      std::clamp(
        props.height, (uint32_t)meta.height.low, (uint32_t)meta.height.high));
}

void
zarr::ZarrV3::allocate_writers_()
{
    writers_.clear();

    for (auto i = 0; i < image_tile_shapes_.size(); ++i) {
        const auto& frame_dims = image_tile_shapes_.at(i).first;
        const auto& tile_dims = image_tile_shapes_.at(i).second;
        const auto& shard_dims_chunks = shard_dims_chunks_.at(i);
        ImageDims shard_dims{
            .cols = shard_dims_chunks.cols * tile_dims.cols,
            .rows = shard_dims_chunks.rows * tile_dims.rows,
        };

        if (blosc_compression_params_.has_value()) {
            writers_.push_back(std::make_shared<ZarrV3Writer>(
              frame_dims,
              shard_dims,
              tile_dims,
              planes_per_chunk_,
              (get_data_directory_() / std::to_string(i)).string(),
              thread_pool_,
              blosc_compression_params_.value()));
        } else {
            writers_.push_back(std::make_shared<ZarrV3Writer>(
              frame_dims,
              shard_dims,
              tile_dims,
              planes_per_chunk_,
              (get_data_directory_() / std::to_string(i)).string(),
              thread_pool_));
        }
    }
}

void
zarr::ZarrV3::set(const StorageProperties* props)
{
    Zarr::set(props);

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    const auto sharding_props = props->shard_dims_chunks;
    const auto sharding_meta = meta.shard_dims_chunks;

    set_sharding(sharding_props, sharding_meta);
}

void
zarr::ZarrV3::get(StorageProperties* props) const
{
    Zarr::get(props);
    if (shard_dims_chunks_.empty()) {
        props->shard_dims_chunks.width = 1;
        props->shard_dims_chunks.height = 1;
    } else {
        const auto& shard_dims_chunks = shard_dims_chunks_.at(0);
        props->shard_dims_chunks.width = shard_dims_chunks.cols;
        props->shard_dims_chunks.height = shard_dims_chunks.rows;
    }
    props->shard_dims_chunks.planes = 1;
}

void
zarr::ZarrV3::get_meta(StoragePropertyMetadata* meta) const
{
    Zarr::get_meta(meta);

    meta->shard_dims_chunks = {
        .is_supported = 1,
        .width = { .writable = 1,
                   .low = 1.f,
                   .high = (float)std::numeric_limits<uint16_t>::max(),
                   .type = PropertyType_FixedPrecision },
        .height = { .writable = 1,
                    .low = 1.f,
                    .high = (float)std::numeric_limits<uint16_t>::max(),
                    .type = PropertyType_FixedPrecision },
        .planes = { .writable = 1,
                    .low = 1.f,
                    .high = 1.f,
                    .type = PropertyType_FixedPrecision },
    };
    meta->multiscale = {
        .is_supported = 0,
    };
}

void
zarr::ZarrV3::reserve_image_shape(const ImageShape* shape)
{
    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);
    image_tile_shapes_.at(0).first = {
        .cols = shape->dims.width,
        .rows = shape->dims.height,
    };
    pixel_type_ = shape->type;

    ImageDims& image_shape = image_tile_shapes_.at(0).first;
    ImageDims& tile_shape = image_tile_shapes_.at(0).second;

    // ensure that tile dimensions are compatible with the image shape
    {
        StorageProperties props = { 0 };
        get(&props);
        uint32_t tile_width = props.chunk_dims_px.width;
        if (image_shape.cols > 0 &&
            (tile_width == 0 || tile_width > image_shape.cols)) {
            LOGE("%s. Setting width to %u.",
                 tile_width == 0 ? "Tile width not specified"
                                 : "Specified tile width is too large",
                 image_shape.cols);
            tile_width = image_shape.cols;
        }
        tile_shape.cols = tile_width;

        uint32_t tile_height = props.chunk_dims_px.height;
        if (image_shape.rows > 0 &&
            (tile_height == 0 || tile_height > image_shape.rows)) {
            LOGE("%s. Setting height to %u.",
                 tile_height == 0 ? "Tile height not specified"
                                  : "Specified tile height is too large",
                 image_shape.rows);
            tile_height = image_shape.rows;
        }
        tile_shape.rows = tile_height;

        storage_properties_destroy(&props);
    }

    const auto& shard_dims_chunks = shard_dims_chunks_.at(0);

    StoragePropertyMetadata meta = { 0 };
    get_meta(&meta);

    const auto shard_width_px = shard_dims_chunks.cols * tile_shape.cols;
    EXPECT(shard_width_px <= image_shape.cols,
           "Shard width %d exceeds frame width %d",
           shard_width_px,
           image_shape.cols);

    const auto shard_height_px = shard_dims_chunks.rows * tile_shape.rows;
    EXPECT(shard_height_px <= image_shape.rows,
           "Shard height %d exceeds frame height %d",
           shard_height_px,
           image_shape.rows);

    allocate_writers_();
}

void
zarr::ZarrV3::write_array_metadata_(size_t level) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (writers_.size() <= level) {
        return;
    }

    const ImageDims& image_dims = image_tile_shapes_.at(level).first;
    const ImageDims& tile_dims = image_tile_shapes_.at(level).second;
    const ImageDims& shard_dims = shard_dims_chunks_.at(level);

    const auto frame_count = writers_.at(level)->frames_written();
    const auto frames_per_chunk = std::min(frame_count, planes_per_chunk_);

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape",
        json::array({
          frames_per_chunk, // t
          1,                // c
          tile_dims.rows,   // y
          tile_dims.cols,   // x
        }) },
      { "separator", "/" },
      { "type", "regular" },
    });
    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = common::sample_type_to_dtype(pixel_type_);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = json::array({
      frame_count,     // t
      1,               // c
      image_dims.rows, // y
      image_dims.cols, // x
    });

    if (blosc_compression_params_.has_value()) {
        auto params = blosc_compression_params_.value();
        metadata["compressor"] = json::object({
          { "codec", "https://purl.org/zarr/spec/codec/blosc/1.0" },
          { "configuration",
            json::object({
              { "blocksize", 0 },
              { "clevel", params.clevel },
              { "cname", params.codec_id },
              { "shuffle", params.shuffle },
            }) },
        });
    }

    // sharding storage transformer
    // TODO (aliddell):
    // https://github.com/zarr-developers/zarr-python/issues/877
    metadata["storage_transformers"] = json::array();
    metadata["storage_transformers"][0] = json::object({
      { "type", "indexed" },
      { "extension",
        "https://purl.org/zarr/spec/storage_transformers/sharding/1.0" },
      { "configuration",
        json::object({
          { "chunks_per_shard",
            json::array({
              1,               // t
              1,               // c
              shard_dims.rows, // y
              shard_dims.cols, // x
            }) },
        }) },
    });

    auto path = (dataset_root_ / "meta" / "root" /
                 (std::to_string(level) + ".array.json"))
                  .string();
    common::write_string(path, metadata.dump(4));
}

/// @brief Write the external metadata.
/// @details This is a no-op for ZarrV3. Instead, external metadata is
/// stored in the group metadata.
void
zarr::ZarrV3::write_external_metadata_() const
{
    // no-op
}

/// @brief Write the metadata for the dataset.
void
zarr::ZarrV3::write_base_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    json metadata;
    metadata["extensions"] = json::array();
    metadata["metadata_encoding"] =
      "https://purl.org/zarr/spec/protocol/core/3.0";
    metadata["metadata_key_suffix"] = ".json";
    metadata["zarr_format"] = "https://purl.org/zarr/spec/protocol/core/3.0";

    auto path = (dataset_root_ / "zarr.json").string();
    common::write_string(path, metadata.dump(4));
}

/// @brief Write the metadata for the group.
/// @details Zarr v3 stores group metadata in
/// /meta/{group_name}.group.json. We will call the group "root".
void
zarr::ZarrV3::write_group_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    json metadata;
    metadata["attributes"]["acquire"] =
      external_metadata_json_.empty() ? ""
                                      : json::parse(external_metadata_json_,
                                                    nullptr, // callback
                                                    true,    // allow exceptions
                                                    true     // ignore comments
                                        );

    auto path = (dataset_root_ / "meta" / "root.group.json").string();
    common::write_string(path, metadata.dump(4));
}

fs::path
zarr::ZarrV3::get_data_directory_() const
{
    return dataset_root_ / "data" / "root";
}

extern "C"
{
    struct Storage* zarr_v3_init()
    {
        try {
            return new zarr::ZarrV3();
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }
    struct Storage* compressed_zarr_v3_zstd_init()
    {
        return compressed_zarr_v3_init<zarr::BloscCodecId::Zstd>();
    }

    struct Storage* compressed_zarr_v3_lz4_init()
    {
        return compressed_zarr_v3_init<zarr::BloscCodecId::Lz4>();
    }
}
