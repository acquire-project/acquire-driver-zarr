#include "zarr.v3.hh"

namespace fs = std::filesystem;
namespace zarr = acquire::sink::zarr;

zarr::ZarrV3::ZarrV3(CompressionParams&& compression_params)
  : Zarr(std::move(compression_params))
{
}

void
zarr::ZarrV3::write_array_metadata_(
  size_t level,
  const ImageShape& image_shape,
  const acquire::sink::zarr::TileShape& tile_shape) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (!writers_.contains(level)) {
        return;
    }

    const uint64_t frame_count = writers_.at(level).front()->frames_written();
    const auto frames_per_chunk =
      std::min(frame_count,
               (uint64_t)get_tiles_per_chunk(
                 image_shape, tile_shape, max_bytes_per_chunk_));

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape",
        json::array({
          frames_per_chunk,
          1,
          tile_shape.height,
          tile_shape.width,
        }) },
      { "separator", std::string(1, dimension_separator_) },
      { "type", "regular" },
    });
    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = sample_type_to_dtype(image_shape.type);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = json::array({
      frame_count,
      image_shape.dims.channels,
      image_shape.dims.height,
      image_shape.dims.width,
    });

    if (compression_params_.has_value()) {
        auto params = compression_params_.value();
        metadata["compressor"] = {
            { "codec", "https://purl.org/zarr/spec/codec/blosc/1.0" },
            { "configuration",
              { { "blocksize", 0 },
                { "clevel", params.clevel_ },
                { "cname", params.codec_id_ },
                { "shuffle", params.shuffle_ } } },
        };
    }

    auto path = (fs::path(dataset_root_) / "meta" / "root" /
                 (std::to_string(level) + ".array.json"))
                  .string();
    write_string(path, metadata.dump(4));
}

/// @brief Write the external metadata.
/// @details This is a no-op for ZarrV3. Instead, external metadata is stored in
/// the group metadata.
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

    auto path = (fs::path(dataset_root_) / "zarr.json").string();
    write_string(path, metadata.dump(4));
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
    metadata["attributes"]["acquire"] = json::parse(external_metadata_json_);

    auto path = (fs::path(dataset_root_) / "meta" / "root.group.json").string();
    write_string(path, metadata.dump(4));
}

std::string
zarr::ZarrV3::get_data_directory_() const
{
    return (fs::path(dataset_root_) / "data" / "root").string();
}

std::string
zarr::ZarrV3::get_chunk_dir_prefix_() const
{
    return "c";
}

extern "C" struct Storage*
zarr_v3_init()
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
