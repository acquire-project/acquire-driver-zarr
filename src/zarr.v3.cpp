#include "zarr.v3.hh"
#include "writers/zarrv3.writer.hh"
#include "writers/sink.creator.hh"

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
zarr::ZarrV3::allocate_writers_()
{
    writers_.clear();

    WriterConfig config = {
        .image_shape = image_shape_,
        .dimensions = acquisition_dimensions_,
        .data_root = dataset_root_ + "/data/root/0",
        .compression_params = blosc_compression_params_,
    };

    writers_.push_back(
      std::make_shared<ZarrV3Writer>(config, thread_pool_, connection_pool_));
    //    if (is_s3_()) {
    //        S3Config s3_config = {
    //            .access_key_id = access_key_id_,
    //            .secret_access_key = secret_access_key_,
    //        };
    //        writers_.push_back(
    //          std::make_shared<ZarrV3S3Writer>(config, s3_config,
    //          thread_pool_));
    //    } else {
    //        writers_.push_back(
    //          std::make_shared<ZarrV3FileWriter>(config, thread_pool_));
    //    }

    if (enable_multiscale_) {
        WriterConfig downsampled_config;

        bool do_downsample = true;
        int level = 1;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);
            writers_.push_back(std::make_shared<ZarrV3Writer>(
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
    meta->multiscale_is_supported = 0;
}

void
zarr::ZarrV3::make_metadata_sinks_()
{
    SinkCreator creator{ thread_pool_, connection_pool_ };
    CHECK(creator.create_v3_metadata_sinks(
      dataset_root_, writers_.size(), metadata_sinks_));
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

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(!metadata_sinks_.empty());
    const std::shared_ptr<Sink>& sink = metadata_sinks_.at(0);
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
    using json = nlohmann::json;

    json metadata;
    metadata["attributes"]["acquire"] =
      external_metadata_json_.empty() ? ""
                                      : json::parse(external_metadata_json_,
                                                    nullptr, // callback
                                                    true,    // allow exceptions
                                                    true     // ignore comments
                                        );

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(metadata_sinks_.size() > 1);
    const std::shared_ptr<Sink>& sink = metadata_sinks_.at(1);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

void
zarr::ZarrV3::write_array_metadata_(size_t level) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    CHECK(level < writers_.size());
    const auto& writer = writers_.at(level);

    const WriterConfig& config = writer->config();
    const auto& image_shape = config.image_shape;

    json metadata;
    metadata["attributes"] = json::object();

    std::vector<size_t> array_shape;
    array_shape.push_back(writer->frames_written());
    for (auto dim = config.dimensions.rbegin() + 1;
         dim != config.dimensions.rend();
         ++dim) {
        array_shape.push_back(dim->array_size_px);
    }

    std::vector<size_t> chunk_shape;
    for (auto dim = config.dimensions.rbegin(); dim != config.dimensions.rend();
         ++dim) {
        chunk_shape.push_back(dim->chunk_size_px);
    }

    std::vector<size_t> shard_shape;
    for (auto dim = config.dimensions.rbegin(); dim != config.dimensions.rend();
         ++dim) {
        shard_shape.push_back(dim->shard_size_chunks);
    }

    metadata["chunk_grid"] = json::object({
      { "chunk_shape", chunk_shape },
      { "separator", "/" },
      { "type", "regular" },
    });

    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = common::sample_type_to_dtype(image_shape.type);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = array_shape;

    if (config.compression_params) {
        const auto params = *config.compression_params;
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
          { "chunks_per_shard", shard_shape },
        }) },
    });

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(metadata_sinks_.size() > 2 + level);
    const std::shared_ptr<Sink>& sink = metadata_sinks_.at(2 + level);
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
