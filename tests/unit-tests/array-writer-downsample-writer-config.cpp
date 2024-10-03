#include "array.writer.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

int
main()
{
    int retval = 1;

    try {
        const fs::path base_dir = "acquire";

        std::vector<ZarrDimension> dims;
        dims.emplace_back("t",
                          ZarrDimensionType_Time,
                          0,
                          5,
                          1); // 5 timepoints / chunk, 1 shard
        dims.emplace_back(
          "c", ZarrDimensionType_Channel, 2, 1, 1); // 2 chunks, 2 shards
        dims.emplace_back(
          "z", ZarrDimensionType_Space, 7, 3, 3); // 3 chunks, 3 shards
        dims.emplace_back(
          "y", ZarrDimensionType_Space, 48, 16, 3); // 3 chunks, 1 shard
        dims.emplace_back(
          "x", ZarrDimensionType_Space, 64, 16, 2); // 4 chunks, 2 shards

        zarr::ArrayWriterConfig config{
            .dimensions = std::make_unique<ArrayDimensions>(std::move(dims),
                                                            ZarrDataType_uint8),
            .dtype = ZarrDataType_uint8,
            .level_of_detail = 0,
            .bucket_name = std::nullopt,
            .store_path = base_dir.string(),
            .compression_params = std::nullopt
        };

        zarr::ArrayWriterConfig downsampled_config;
        CHECK(zarr::downsample(config, downsampled_config));

        // check dimensions
        CHECK(downsampled_config.dimensions->ndims() == 5);

        CHECK(downsampled_config.dimensions->at(0).name == "t");
        CHECK(downsampled_config.dimensions->at(0).array_size_px == 0);
        CHECK(downsampled_config.dimensions->at(0).chunk_size_px == 5);
        CHECK(downsampled_config.dimensions->at(0).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(1).name == "c");
        // we don't downsample channels
        CHECK(downsampled_config.dimensions->at(1).array_size_px == 2);
        CHECK(downsampled_config.dimensions->at(1).chunk_size_px == 1);
        CHECK(downsampled_config.dimensions->at(1).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(2).name == "z");
        CHECK(downsampled_config.dimensions->at(2).array_size_px == 4);
        CHECK(downsampled_config.dimensions->at(2).chunk_size_px == 3);
        CHECK(downsampled_config.dimensions->at(2).shard_size_chunks == 2);

        CHECK(downsampled_config.dimensions->at(3).name == "y");
        CHECK(downsampled_config.dimensions->at(3).array_size_px == 24);
        CHECK(downsampled_config.dimensions->at(3).chunk_size_px == 16);
        CHECK(downsampled_config.dimensions->at(3).shard_size_chunks == 2);

        CHECK(downsampled_config.dimensions->at(4).name == "x");
        CHECK(downsampled_config.dimensions->at(4).array_size_px == 32);
        CHECK(downsampled_config.dimensions->at(4).chunk_size_px == 16);
        CHECK(downsampled_config.dimensions->at(4).shard_size_chunks == 2);

        // check level of detail
        CHECK(downsampled_config.level_of_detail == 1);

        // check store path
        CHECK(downsampled_config.store_path == config.store_path);

        // check compression params
        CHECK(!downsampled_config.compression_params.has_value());

        // downsample again
        config = std::move(downsampled_config);

        // can't downsample anymore
        CHECK(!zarr::downsample(config, downsampled_config));

        // check dimensions
        CHECK(downsampled_config.dimensions->ndims() == 5);

        CHECK(downsampled_config.dimensions->at(0).name == "t");
        CHECK(downsampled_config.dimensions->at(0).array_size_px == 0);
        CHECK(downsampled_config.dimensions->at(0).chunk_size_px == 5);
        CHECK(downsampled_config.dimensions->at(0).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(1).name == "c");
        // we don't downsample channels
        CHECK(downsampled_config.dimensions->at(1).array_size_px == 2);
        CHECK(downsampled_config.dimensions->at(1).chunk_size_px == 1);
        CHECK(downsampled_config.dimensions->at(1).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(2).name == "z");
        CHECK(downsampled_config.dimensions->at(2).array_size_px == 2);
        CHECK(downsampled_config.dimensions->at(2).chunk_size_px == 2);
        CHECK(downsampled_config.dimensions->at(2).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(3).name == "y");
        CHECK(downsampled_config.dimensions->at(3).array_size_px == 12);
        CHECK(downsampled_config.dimensions->at(3).chunk_size_px == 12);
        CHECK(downsampled_config.dimensions->at(3).shard_size_chunks == 1);

        CHECK(downsampled_config.dimensions->at(4).name == "x");
        CHECK(downsampled_config.dimensions->at(4).array_size_px == 16);
        CHECK(downsampled_config.dimensions->at(4).chunk_size_px == 16);
        CHECK(downsampled_config.dimensions->at(4).shard_size_chunks == 1);

        // check level of detail
        CHECK(downsampled_config.level_of_detail == 2);

        // check data root
        CHECK(downsampled_config.store_path == config.store_path);

        // check compression params
        CHECK(!downsampled_config.compression_params.has_value());

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}