#include "zarr.v2.hh"
#include "writers/zarrv2.array.writer.hh"
#include "writers/sink.creator.hh"

#include "nlohmann/json.hpp"

#include <bit>

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
}

void
zarr::ZarrV2::allocate_writers_()
{
    writers_.clear();

    ArrayWriterConfig config = {
        .image_shape = image_shape_,
        .dimensions = acquisition_dimensions_,
        .level_of_detail = 0,
        .dataset_root = dataset_root_,
        .compression_params = blosc_compression_params_,
    };

    writers_.push_back(std::make_shared<ZarrV2ArrayWriter>(
      config, thread_pool_, connection_pool_));

    if (enable_multiscale_) {
        ArrayWriterConfig downsampled_config;

        bool do_downsample = true;
        int level = 1;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);
            writers_.push_back(std::make_shared<ZarrV2ArrayWriter>(
              downsampled_config, thread_pool_, connection_pool_));
            scaled_frames_.emplace(level++, std::nullopt);

            config = std::move(downsampled_config);
            downsampled_config = {};
        }
    }
}

void
zarr::ZarrV2::make_metadata_sinks_()
{
    SinkCreator creator(thread_pool_, connection_pool_);
    CHECK(creator.make_metadata_sinks(
      ZarrVersion::V2, dataset_root_, metadata_sinks_));
}

void
zarr::ZarrV2::write_base_metadata_() const
{
    namespace fs = std::filesystem;

    const json metadata = { { "zarr_format", 2 } };
    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(".metadata");
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
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at("0/.zattrs");
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
    const std::unique_ptr<Sink>& sink = metadata_sinks_.at(".zattrs");
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
