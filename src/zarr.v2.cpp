#include "zarr.v2.hh"
#include "writers/zarrv2.writer.hh"

#include "json.hpp"

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
        .data_root = (dataset_root_ / "0").string(),
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
zarr::ZarrV2::write_array_metadata_(size_t level) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

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

    if (config.compression_params.has_value()) {
        metadata["compressor"] = config.compression_params.value();
    } else {
        metadata["compressor"] = nullptr;
    }

    std::string zarray_path = (fs::path(config.data_root) / ".zarray").string();
    common::write_string(zarray_path, metadata.dump());
}

void
zarr::ZarrV2::write_external_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (dataset_root_ / "0" / ".zattrs").string();
    std::string external_metadata = external_metadata_json_.empty()
                                      ? ""
                                      : json::parse(external_metadata_json_,
                                                    nullptr, // callback
                                                    true,    // allow exceptions
                                                    true     // ignore comments
                                                    )
                                          .dump();
    common::write_string(zattrs_path, external_metadata);
}

void
zarr::ZarrV2::write_base_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (dataset_root_ / ".zgroup").string();
    common::write_string(zgroup_path, zgroup.dump());
}

void
zarr::ZarrV2::write_group_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    json zgroup_attrs;
    zgroup_attrs["multiscales"] = json::array({ json::object() });
    zgroup_attrs["multiscales"][0]["version"] = "0.4";
    zgroup_attrs["multiscales"][0]["axes"] = {
        {
          { "name", "t" },
          { "type", "time" },
        },
        {
          { "name", "c" },
          { "type", "channel" },
        },
        {
          { "name", "y" },
          { "type", "space" },
          { "unit", "micrometer" },
        },
        {
          { "name", "x" },
          { "type", "space" },
          { "unit", "micrometer" },
        },
    };

    // spatial multiscale metadata
    if (writers_.empty()) {
        zgroup_attrs["multiscales"][0]["datasets"] = {
            {
              { "path", "0" },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale",
                      {
                        1,                 // t
                        1,                 // c
                        pixel_scale_um_.y, // y
                        pixel_scale_um_.x  // x
                      } },
                  },
                } },
            },
        };
    } else {
        for (auto i = 0; i < writers_.size(); ++i) {
            zgroup_attrs["multiscales"][0]["datasets"].push_back({
              { "path", std::to_string(i) },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    {
                      "scale",
                      {
                        std::pow(2, i),                     // t
                        1,                                  // c
                        std::pow(2, i) * pixel_scale_um_.y, // y
                        std::pow(2, i) * pixel_scale_um_.x  // x
                      },
                    },
                  },
                } },
            });
        }

        // downsampling metadata
        zgroup_attrs["multiscales"][0]["type"] = "local_mean";
        zgroup_attrs["multiscales"][0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters are "
              "given here." },
            { "method", "skimage.transform.downscale_local_mean" },
            { "version", "0.21.0" },
            { "args", "[2]" },
            { "kwargs", { "cval", 0 } },
        };
    }

    std::string zattrs_path = (dataset_root_ / ".zattrs").string();
    common::write_string(zattrs_path, zgroup_attrs.dump(4));
}

fs::path
zarr::ZarrV2::get_data_directory_() const
{
    return dataset_root_;
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
