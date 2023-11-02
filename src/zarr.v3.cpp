#include "zarr.v3.hh"
#include "writers/shard.writer.hh"

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

/** @brief Find the smallest prime factor of @p n. **/
uint32_t
smallest_prime_factor(uint32_t n)
{
    if (n < 2) {
        return 1;
    } else if (n % 2 == 0) {
        return 2;
    }

    // collect additional primes
    std::vector<uint32_t> primes = { 3, 5, 7, 11, 13, 17, 19, 23 };
    for (auto i = 27; i * i <= n; i += 2) {
        bool is_prime = true;
        for (auto p : primes) {
            if (i % p == 0) {
                is_prime = false;
                break;
            }
        }
        if (is_prime) {
            primes.push_back(i);
        }
    }

    for (auto p : primes) {
        if (n % p == 0) {
            return p;
        }
    }

    return n;
}

/**
 * @brief Compute the shard dimensions for a given frame and tile shape.
 * @details The shard dimensions are computed by determining how many tiles are
 *          needed to cover the frame, and then computing the minimum number of
 *          shards that can be used to cover the tiles. We are constrained by
 *          the shard dimensions being a multiple of the tile dimensions, and by
 *          the shard dimensions not exceeding the frame dimensions. We also
 *          prefer to write to as few shards (i.e., files) as possible.
 * @param frame_dims Width and height of the frame.
 * @param tile_dims Width and height of each tile.
 * @return Width and height of each shard.
 */
zarr::ImageDims
make_shard_dims(const zarr::ImageDims& frame_dims,
                const zarr::ImageDims& tile_dims)
{
    zarr::ImageDims shard_dims = {
        .cols = frame_dims.cols,
        .rows = frame_dims.rows,
    };

    const auto h_rat = (float)frame_dims.rows / (float)tile_dims.rows;

    // number of pixel rows across all shards
    auto shard_rows = (uint32_t)std::ceil(h_rat * tile_dims.rows);

    // if the number of rows is larger than that of the frame, we need to split
    // them up
    if (shard_rows > frame_dims.rows) {
        auto n_shards_rows = smallest_prime_factor(shard_rows / tile_dims.rows);
        shard_dims.rows = n_shards_rows * tile_dims.rows;
    }

    const auto w_rat = (float)frame_dims.cols / (float)tile_dims.cols;

    // number of pixel columns across all shards
    auto shard_cols = (uint32_t)std::ceil(w_rat * tile_dims.cols);

    // if the number of columns is larger than that of the frame, we need to
    // split them up
    if (shard_cols > frame_dims.cols) {
        auto n_shards_cols = smallest_prime_factor(shard_cols / tile_dims.cols);
        shard_dims.cols = n_shards_cols * tile_dims.cols;
    }

    return shard_dims;
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
    shard_dims_.reserve(image_tile_shapes_.size());

    for (auto i = 0; i < image_tile_shapes_.size(); ++i) {
        const auto& frame_dims = image_tile_shapes_.at(i).first;
        const auto& tile_dims = image_tile_shapes_.at(i).second;

        const uint64_t bytes_per_tile =
          common::bytes_per_tile(tile_dims, pixel_type_);

        const auto shard_dims = make_shard_dims(frame_dims, tile_dims);
        shard_dims_.push_back(shard_dims);

        if (blosc_compression_params_.has_value()) {
            writers_.push_back(std::make_shared<ShardWriter>(
              frame_dims,
              shard_dims,
              tile_dims,
              (uint32_t)(max_bytes_per_chunk_ / bytes_per_tile),
              (get_data_directory_() / std::to_string(i)).string(),
              this,
              blosc_compression_params_.value()));
        } else {
            writers_.push_back(std::make_shared<ShardWriter>(
              frame_dims,
              shard_dims,
              tile_dims,
              (uint32_t)(max_bytes_per_chunk_ / bytes_per_tile),
              (get_data_directory_() / std::to_string(i)).string(),
              this));
        }
    }
}

void
zarr::ZarrV3::get_meta(StoragePropertyMetadata* meta) const
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
zarr::ZarrV3::write_array_metadata_(size_t level) const
{
    namespace fs = std::filesystem;
    using json = nlohmann::json;

    if (writers_.size() <= level) {
        return;
    }

    const ImageDims& image_dims = image_tile_shapes_.at(level).first;
    const ImageDims& tile_dims = image_tile_shapes_.at(level).second;
    const ImageDims& shard_dims = shard_dims_.at(level);

    const uint64_t frame_count = writers_.at(level)->frames_written();
    const auto frames_per_chunk =
      std::min(frame_count,
               (uint64_t)common::frames_per_chunk(
                 tile_dims, pixel_type_, max_bytes_per_chunk_));

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape",
        json::array({
          frames_per_chunk, // t
                            // TODO (aliddell): c?
          1,                // z
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
                       // TODO (aliddell): c?
      1,               // z
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
              1,                                 // t
                                                 // TODO (aliddell): c?
              1,                                 // z
              shard_dims_.rows / tile_dims.rows, // y
              shard_dims_.cols / tile_dims.cols, // x
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
    metadata["attributes"]["acquire"] = json::parse(external_metadata_json_);

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
