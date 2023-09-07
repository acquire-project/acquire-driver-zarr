#include "zarr.v2.hh"

namespace fs = std::filesystem;
namespace zarr = acquire::sink::zarr;

zarr::ZarrV2::ZarrV2(CompressionParams&& compression_params)
  : Zarr(std::move(compression_params))
{
}

void
zarr::ZarrV2::write_array_metadata_(size_t level,
                                  const ImageShape& image_shape,
                                  const TileShape& tile_shape) const
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

    json zarray_attrs = {
        { "zarr_format", 2 },
        { "shape",
          {
            frame_count,
            image_shape.dims.channels,
            image_shape.dims.height,
            image_shape.dims.width,
          } },
        { "chunks",
          {
            frames_per_chunk,
            1,
            tile_shape.height,
            tile_shape.width,
          } },
        { "dtype", sample_type_to_dtype(image_shape.type) },
        { "fill_value", 0 },
        { "order", "C" },
        { "filters", nullptr },
        { "dimension_separator", std::string(1, dimension_separator_) },
    };

    if (compression_params_.has_value())
        zarray_attrs["compressor"] = compression_params_.value();
    else
        zarray_attrs["compressor"] = nullptr;

    std::string zarray_path =
      (fs::path(dataset_root_) / std::to_string(level) / ".zarray").string();
    write_string(zarray_path, zarray_attrs.dump());
}

void
zarr::ZarrV2::write_external_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (fs::path(dataset_root_) / "0" / ".zattrs").string();
    write_string(zattrs_path, external_metadata_json_);
}

void
zarr::ZarrV2::write_base_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (fs::path(dataset_root_) / ".zgroup").string();
    write_string(zgroup_path, zgroup.dump());
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
    if (writers_.empty() || !frame_scaler_.has_value()) {
        zgroup_attrs["multiscales"][0]["datasets"] = {
            {
              { "path", "0" },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale", { 1, 1, pixel_scale_um_.y, pixel_scale_um_.x } },
                  },
                } },
            },
        };
    } else {
        for (const auto& [layer, _] : writers_) {
            zgroup_attrs["multiscales"][0]["datasets"].push_back({
              { "path", std::to_string(layer) },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale",
                      { std::pow(2, layer),
                        1,
                        std::pow(2, layer) * pixel_scale_um_.y,
                        std::pow(2, layer) * pixel_scale_um_.x } },
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

    std::string zattrs_path = (fs::path(dataset_root_) / ".zattrs").string();
    write_string(zattrs_path, zgroup_attrs.dump(4));
}

std::string
zarr::ZarrV2::get_data_directory_() const
{
    return dataset_root_;
}

std::string
zarr::ZarrV2::get_chunk_dir_prefix_() const
{
    return "";
}

extern "C" struct Storage*
zarr_v2_init()
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