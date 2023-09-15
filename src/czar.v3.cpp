#include "czar.v3.hh"

#include "json.hpp"

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_v3_init()
{
    try {
        zarr::CompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::CzarV3(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

/// CzarV3
zarr::CzarV3::CzarV3(CompressionParams&& compression_params)
  : Czar(std::move(compression_params))
{
}

void
zarr::CzarV3::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    *meta = {
        .chunking = {
          .supported = 1,
          .max_bytes_per_chunk = {
            .writable = 1,
            .low = (float)(16 << 20),
            .high = (float)(1 << 30),
            .type = PropertyType_FixedPrecision },
        },
        .multiscale = {
          .supported = 0,
        }
    };
}

void
zarr::CzarV3::allocate_writers_()
{
    uint64_t bytes_per_tile = common::bytes_per_tile(tile_shape_, pixel_type_);

    writers_.clear();
    writers_.push_back(std::make_shared<ChonkWriter>(
      image_shape_,
      tile_shape_,
      (uint32_t)(max_bytes_per_chunk_ / bytes_per_tile),
      dataset_root_.string()));
}

void
zarr::CzarV3::write_array_metadata_(size_t level,
                                    const ImageDims& image_shape,
                                    const ImageDims& tile_shape) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (writers_.size() <= level) {
        return;
    }

    const uint64_t frame_count = writers_.at(level)->frames_written();
    const auto frames_per_chunk =
      std::min(frame_count,
               (uint64_t)common::frames_per_chunk(
                 tile_shape, pixel_type_, max_bytes_per_chunk_));

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape",
        json::array({
          frames_per_chunk, // t
                            // TODO (aliddell): c?
          1,                // z
          tile_shape.rows,  // y
          tile_shape.cols,  // x
        }) },
      { "separator", "/" },
      { "type", "regular" },
    });
    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = common::sample_type_to_dtype(pixel_type_);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = json::array({
      frame_count,      // t
                        // TODO (aliddell): c?
      1,                // z
      image_shape.rows, // y
      image_shape.cols, // x
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

    auto path = (dataset_root_ / "meta" / "root" /
                 (std::to_string(level) + ".array.json"))
                  .string();
    common::write_string(path, metadata.dump(4));
}

/// @brief Write the external metadata.
/// @details This is a no-op for CzarV3. Instead, external metadata is
/// stored in the group metadata.
void
zarr::CzarV3::write_external_metadata_() const
{
    // no-op
}

/// @brief Write the metadata for the dataset.
void
zarr::CzarV3::write_base_metadata_() const
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
zarr::CzarV3::write_group_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    json metadata;
    metadata["attributes"]["acquire"] = json::parse(external_metadata_json_);

    auto path = (dataset_root_ / "meta" / "root.group.json").string();
    common::write_string(path, metadata.dump(4));
}

std::string
zarr::CzarV3::get_data_directory_() const
{
    return (dataset_root_ / "data" / "root").string();
}

std::string
zarr::CzarV3::get_chunk_dir_prefix_() const
{
    return "c";
}

extern "C"
{
    struct Storage* zarr_v3_init()
    {
        try {
            return new zarr::CzarV3();
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
