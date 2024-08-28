#include "array.writer.hh"
#include "logger.hh"

#include <filesystem>

namespace fs = std::filesystem;

int
main()
{
    int retval = 1;

    try {
        const fs::path base_dir = "acquire";

        zarr::ArrayWriterConfig config{ .dimensions = {},
                                        .dtype = ZarrDataType_uint8,
                                        .level_of_detail = 0,
                                        .dataset_root = base_dir.string(),
                                        .compression_params = std::nullopt };

        config.dimensions.emplace_back("t",
                                       ZarrDimensionType_Time,
                                       0,
                                       5,
                                       1); // 5 timepoints / chunk, 1 shard
        config.dimensions.emplace_back(
          "c", ZarrDimensionType_Channel, 2, 1, 1); // 2 chunks, 2 shards
        config.dimensions.emplace_back(
          "z", ZarrDimensionType_Space, 7, 3, 3); // 3 chunks, 3 shards
        config.dimensions.emplace_back(
          "y", ZarrDimensionType_Space, 48, 16, 3); // 3 chunks, 1 shard
        config.dimensions.emplace_back(
          "x", ZarrDimensionType_Space, 64, 16, 2); // 4 chunks, 2 shards

        zarr::ArrayWriterConfig downsampled_config;
        CHECK(zarr::downsample(config, downsampled_config));

        // check dimensions
        CHECK(downsampled_config.dimensions.size() == 5);

        CHECK(downsampled_config.dimensions[0].name == "t");
        CHECK(downsampled_config.dimensions[0].array_size_px == 0);
        CHECK(downsampled_config.dimensions[0].chunk_size_px == 5);
        CHECK(downsampled_config.dimensions[0].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[1].name == "c");
        // we don't downsample channels
        CHECK(downsampled_config.dimensions[1].array_size_px == 2);
        CHECK(downsampled_config.dimensions[1].chunk_size_px == 1);
        CHECK(downsampled_config.dimensions[1].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[2].name == "z");
        CHECK(downsampled_config.dimensions[2].array_size_px == 4);
        CHECK(downsampled_config.dimensions[2].chunk_size_px == 3);
        CHECK(downsampled_config.dimensions[2].shard_size_chunks == 2);

        CHECK(downsampled_config.dimensions[3].name == "y");
        CHECK(downsampled_config.dimensions[3].array_size_px == 24);
        CHECK(downsampled_config.dimensions[3].chunk_size_px == 16);
        CHECK(downsampled_config.dimensions[3].shard_size_chunks == 2);

        CHECK(downsampled_config.dimensions[4].name == "x");
        CHECK(downsampled_config.dimensions[4].array_size_px == 32);
        CHECK(downsampled_config.dimensions[4].chunk_size_px == 16);
        CHECK(downsampled_config.dimensions[4].shard_size_chunks == 2);

        // check level of detail
        CHECK(downsampled_config.level_of_detail == 1);

        // check dataset root
        CHECK(downsampled_config.dataset_root == config.dataset_root);

        // check compression params
        CHECK(!downsampled_config.compression_params.has_value());

        // downsample again
        config = std::move(downsampled_config);

        // can't downsample anymore
        CHECK(!zarr::downsample(config, downsampled_config));

        // check dimensions
        CHECK(downsampled_config.dimensions.size() == 5);

        CHECK(downsampled_config.dimensions[0].name == "t");
        CHECK(downsampled_config.dimensions[0].array_size_px == 0);
        CHECK(downsampled_config.dimensions[0].chunk_size_px == 5);
        CHECK(downsampled_config.dimensions[0].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[1].name == "c");
        // we don't downsample channels
        CHECK(downsampled_config.dimensions[1].array_size_px == 2);
        CHECK(downsampled_config.dimensions[1].chunk_size_px == 1);
        CHECK(downsampled_config.dimensions[1].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[2].name == "z");
        CHECK(downsampled_config.dimensions[2].array_size_px == 2);
        CHECK(downsampled_config.dimensions[2].chunk_size_px == 2);
        CHECK(downsampled_config.dimensions[2].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[3].name == "y");
        CHECK(downsampled_config.dimensions[3].array_size_px == 12);
        CHECK(downsampled_config.dimensions[3].chunk_size_px == 12);
        CHECK(downsampled_config.dimensions[3].shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions[4].name == "x");
        CHECK(downsampled_config.dimensions[4].array_size_px == 16);
        CHECK(downsampled_config.dimensions[4].chunk_size_px == 16);
        CHECK(downsampled_config.dimensions[4].shard_size_chunks == 1);

        // check level of detail
        CHECK(downsampled_config.level_of_detail == 2);

        // check data root
        CHECK(downsampled_config.dataset_root == config.dataset_root);

        // check compression params
        CHECK(!downsampled_config.compression_params.has_value());

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}