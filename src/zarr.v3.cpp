#include "zarr.v3.hh"
#include "writers/zarrv3.array.writer.hh"
#include "writers/sink.creator.hh"

#include "nlohmann/json.hpp"

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
zarr::ZarrV3::allocate_writers_()
{
    writers_.clear();

    ArrayWriterConfig config = {
        .image_shape = image_shape_,
        .dimensions = acquisition_dimensions_,
        .level_of_detail = 0,
        .dataset_root = dataset_root_,
        .compression_params = blosc_compression_params_,
    };

    writers_.push_back(std::make_shared<ZarrV3ArrayWriter>(
      config, thread_pool_, connection_pool_));

    if (enable_multiscale_) {
        ArrayWriterConfig downsampled_config;

        bool do_downsample = true;
        int level = 1;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);
            writers_.push_back(std::make_shared<ZarrV3ArrayWriter>(
              downsampled_config, thread_pool_, connection_pool_));
            scaled_frames_.emplace(level++, std::nullopt);

            config = std::move(downsampled_config);
            downsampled_config = {};
        }
    }
}

void
zarr::ZarrV3::get_meta(StoragePropertyMetadata* meta) const
{
    Zarr::get_meta(meta);
    meta->sharding_is_supported = 1;
}

void
zarr::ZarrV3::make_metadata_sinks_()
{
    SinkCreator creator(thread_pool_, connection_pool_);
    CHECK(creator.make_metadata_sinks(
      ZarrVersion::V3, dataset_root_, metadata_sinks_));
}

/// @brief Write the metadata for the dataset.
void
zarr::ZarrV3::write_base_metadata_() const
{
    namespace fs = std::filesystem;

    json metadata;
    metadata["extensions"] = json::array();
    metadata["metadata_encoding"] =
      "https://purl.org/zarr/spec/protocol/core/3.0";
    metadata["metadata_key_suffix"] = ".json";
    metadata["zarr_format"] = "https://purl.org/zarr/spec/protocol/core/3.0";

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at("zarr.json");
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

/// @brief Write the external metadata.
/// @details This is a no-op for ZarrV3. Instead, external metadata is
/// stored in the group metadata.
void
zarr::ZarrV3::write_external_metadata_() const
{
    // no-op
}

/// @brief Write the metadata for the group.
/// @details Zarr v3 stores group metadata in
/// /meta/{group_name}.group.json. We will call the group "root".
void
zarr::ZarrV3::write_group_metadata_() const
{
    namespace fs = std::filesystem;

    json metadata;
    metadata["attributes"]["acquire"] =
      external_metadata_json_.empty() ? ""
                                      : json::parse(external_metadata_json_,
                                                    nullptr, // callback
                                                    true,    // allow exceptions
                                                    true     // ignore comments
                                        );
    metadata["attributes"]["multiscales"] = make_multiscale_metadata_();

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    const std::unique_ptr<Sink>& sink =
      metadata_sinks_.at("meta/root.group.json");
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
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
