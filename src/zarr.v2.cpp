#include "zarr.v2.hh"
#include "writers/zarrv2.writer.hh"
#include "writers/sink.creator.hh"

#include "nlohmann/json.hpp"

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_v2_init()
{
    try {
        zarr::BloscCompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::ZarrV2(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

/// ZarrV2
zarr::ZarrV2::ZarrV2(BloscCompressionParams&& compression_params)
  : Zarr(std::move(compression_params))
{
}

void
zarr::ZarrV2::get_meta(StoragePropertyMetadata* meta) const
{
    Zarr::get_meta(meta);
    meta->sharding_is_supported = 0;
    meta->multiscale_is_supported = 1;
}

void
zarr::ZarrV2::allocate_writers_()
{
    writers_.clear();

    WriterConfig config = {
        .image_shape = image_shape_,
        .dimensions = acquisition_dimensions_,
        .data_root = dataset_root_ + "/0",
        .compression_params = blosc_compression_params_,
    };

    writers_.push_back(std::make_shared<ZarrV2Writer>(config, thread_pool_));

    if (enable_multiscale_) {
        WriterConfig downsampled_config;

        bool do_downsample = true;
        int level = 1;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);
            writers_.push_back(
              std::make_shared<ZarrV2Writer>(downsampled_config, thread_pool_));
            scaled_frames_.emplace(level++, std::nullopt);

            config = std::move(downsampled_config);
            downsampled_config = {};
        }
    }
}

void
zarr::ZarrV2::make_metadata_sinks_()
{
    SinkCreator creator(thread_pool_);
    CHECK(creator.create_v2_metadata_sinks(
      dataset_root_, writers_.size(), metadata_sinks_));
}

void
zarr::ZarrV2::write_base_metadata_() const
{
    namespace fs = std::filesystem;

    const json metadata = { { "zarr_format", 2 } };
    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(!metadata_sinks_.empty());
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(0);
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

void
zarr::ZarrV2::write_external_metadata_() const
{
    namespace fs = std::filesystem;

    std::string metadata_str = external_metadata_json_.empty()
                                 ? "{}"
                                 : json::parse(external_metadata_json_,
                                               nullptr, // callback
                                               true,    // allow exceptions
                                               true     // ignore comments
                                               )
                                     .dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(metadata_sinks_.size() > 1);
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(1);
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

void
zarr::ZarrV2::write_group_metadata_() const
{
    namespace fs = std::filesystem;

    json metadata;
    metadata["multiscales"] = make_multiscale_metadata_();

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(metadata_sinks_.size() > 2);
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(2);
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

void
zarr::ZarrV2::write_array_metadata_(size_t level) const
{
    namespace fs = std::filesystem;

    CHECK(level < writers_.size());
    const auto& writer = writers_.at(level);

    const WriterConfig& config = writer->config();
    const auto& image_shape = config.image_shape;

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

    json metadata;
    metadata["zarr_format"] = 2;
    metadata["shape"] = array_shape;
    metadata["chunks"] = chunk_shape;
    metadata["dtype"] = common::sample_type_to_dtype(image_shape.type);
    metadata["fill_value"] = 0;
    metadata["order"] = "C";
    metadata["filters"] = nullptr;
    metadata["dimension_separator"] = "/";

    if (config.compression_params) {
        metadata["compressor"] = *config.compression_params;
    } else {
        metadata["compressor"] = nullptr;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    CHECK(metadata_sinks_.size() > 3 + level);
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(3 + level);
    CHECK(sink);
    CHECK(sink->write(0, metadata_bytes, metadata_str.size()));
}

extern "C"
{
    struct Storage* zarr_v2_init()
    {
        try {
            return new zarr::ZarrV2();
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
        return nullptr;
    }
    struct Storage* compressed_zarr_v2_zstd_init()
    {
        return compressed_zarr_v2_init<zarr::BloscCodecId::Zstd>();
    }

    struct Storage* compressed_zarr_v2_lz4_init()
    {
        return compressed_zarr_v2_init<zarr::BloscCodecId::Lz4>();
    }
}
