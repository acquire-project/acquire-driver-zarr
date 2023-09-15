#include "czar.v2.hh"

#include "json.hpp"

namespace zarr = acquire::sink::zarr;

namespace {
template<zarr::BloscCodecId CodecId>
struct Storage*
compressed_zarr_v2_init()
{
    try {
        zarr::CompressionParams params(
          zarr::compression_codec_as_string<CodecId>(), 1, 1);
        return new zarr::CzarV2(std::move(params));
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
    return nullptr;
}
} // end ::{anonymous} namespace

/// CzarV2
zarr::CzarV2::CzarV2(CompressionParams&& compression_params)
  : Czar(std::move(compression_params))
{
}

void
zarr::CzarV2::get_meta(StoragePropertyMetadata* meta) const
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
          .supported = 1,
        }
    };
}

void
zarr::CzarV2::allocate_writers_()
{
    uint64_t bytes_per_tile = common::bytes_per_tile(tile_shape_, pixel_type_);

    writers_.clear();
    writers_.push_back(std::make_shared<ChonkWriter>(
      image_shape_,
      tile_shape_,
      (uint32_t)(max_bytes_per_chunk_ / bytes_per_tile),
      (dataset_root_ / "0").string()));

    if (enable_multiscale_) {
    }
}

void
zarr::CzarV2::write_array_metadata_(size_t level,
                                    const ImageDims& image_shape,
                                    const ImageDims& tile_shape) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (writers_.size() <= level) {
        return;
    }

    const auto frame_count = (uint64_t)writers_.at(level)->frames_written();
    const auto frames_per_chunk =
      std::min(frame_count,
               (uint64_t)common::frames_per_chunk(
                 tile_shape, pixel_type_, max_bytes_per_chunk_));

    json zarray_attrs = {
        { "zarr_format", 2 },
        { "shape",
          {
            frame_count,      // t
                              // TODO (aliddell): c?
            1,                // z
            image_shape.rows, // y
            image_shape.cols, // x
          } },
        { "chunks",
          {
            frames_per_chunk, // t
                              // TODO (aliddell): c?
            1,                // z
            tile_shape.rows,  // y
            tile_shape.cols,  // x
          } },
        { "dtype", common::sample_type_to_dtype(pixel_type_) },
        { "fill_value", 0 },
        { "order", "C" },
        { "filters", nullptr },
        { "dimension_separator", "/" },
    };

    if (compression_params_.has_value()) {
        zarray_attrs["compressor"] = compression_params_.value();
    } else {
        zarray_attrs["compressor"] = nullptr;
    }

    std::string zarray_path =
      (dataset_root_ / std::to_string(level) / ".zarray").string();
    common::write_string(zarray_path, zarray_attrs.dump());
}

void
zarr::CzarV2::write_external_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    std::string zattrs_path = (dataset_root_ / "0" / ".zattrs").string();
    common::write_string(zattrs_path, external_metadata_json_);
}

void
zarr::CzarV2::write_base_metadata_() const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const json zgroup = { { "zarr_format", 2 } };
    std::string zgroup_path = (dataset_root_ / ".zgroup").string();
    common::write_string(zgroup_path, zgroup.dump());
}

void
zarr::CzarV2::write_group_metadata_() const
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
    //    if (writers_.empty() || !frame_scaler_.has_value()) {
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
    //    } else {
    //        for (auto i = 0; i < writers_.size(); ++i) {
    //            zgroup_attrs["multiscales"][0]["datasets"].push_back({
    //              { "path", std::to_string(i) },
    //              { "coordinateTransformations",
    //                {
    //                  {
    //                    { "type", "scale" },
    //                    {
    //                      "scale",
    //                      {
    //                        std::pow(2, i), // t
    //                                        // TODO (aliddell): c?
    //                        1,              // z
    //                        std::pow(2, i) * pixel_scale_um_.y, // y
    //                        std::pow(2, i) * pixel_scale_um_.x  // x
    //                      },
    //                    },
    //                  },
    //                } },
    //            });
    //        }
    //
    //        // downsampling metadata
    //        zgroup_attrs["multiscales"][0]["type"] = "local_mean";
    //        zgroup_attrs["multiscales"][0]["metadata"] = {
    //            { "description",
    //              "The fields in the metadata describe how to reproduce this "
    //              "multiscaling in scikit-image. The method and its parameters
    //              are " "given here." },
    //            { "method", "skimage.transform.downscale_local_mean" },
    //            { "version", "0.21.0" },
    //            { "args", "[2]" },
    //            { "kwargs", { "cval", 0 } },
    //        };
    //    }

    std::string zattrs_path = (dataset_root_ / ".zattrs").string();
    common::write_string(zattrs_path, zgroup_attrs.dump(4));
}

std::string
zarr::CzarV2::get_data_directory_() const
{
    return dataset_root_.string();
}

std::string
zarr::CzarV2::get_chunk_dir_prefix_() const
{
    return "";
}

extern "C"
{
    struct Storage* zarr_v2_init()
    {
        try {
            return new zarr::CzarV2();
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
