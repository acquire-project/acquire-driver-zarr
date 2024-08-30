#include "zarrv3.array.writer.hh"
#include "logger.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <filesystem>

#define EXPECT_EQ(a, b)                                                        \
    EXPECT((a) == (b), "Expected %s == %s, but %zu != %zu", #a, #b, a, b)

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

const unsigned int array_width = 64, array_height = 48, array_planes = 6,
                   array_channels = 8, array_timepoints = 10;
const unsigned int n_frames = array_planes * array_channels * array_timepoints;

const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                   chunk_channels = 4, chunk_timepoints = 5;

const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                   shard_channels = 2, shard_timepoints = 2;
const unsigned int chunks_per_shard =
  shard_width * shard_height * shard_planes * shard_channels * shard_timepoints;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
const unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
const unsigned int chunks_in_c =
  (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

const unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
const unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
const unsigned int shards_in_z =
  (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
const unsigned int shards_in_c =
  (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
const unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

const int level_of_detail = 3;
} // namespace

void
check_json()
{
    fs::path meta_path = base_dir / "meta" / "root" /
                         (std::to_string(level_of_detail) + ".array.json");
    CHECK(fs::is_regular_file(meta_path));

    std::ifstream f(meta_path);
    nlohmann::json meta = nlohmann::json::parse(f);

    EXPECT(meta["data_type"].get<std::string>() == "uint16",
           "Expected dtype to be 'uint16', but got '%s'",
           meta["data_type"].get<std::string>().c_str());

    const auto& array_shape = meta["shape"];
    const auto& chunk_shape = meta["chunk_grid"]["chunk_shape"];
    const auto& shard_shape =
      meta["storage_transformers"][0]["configuration"]["chunks_per_shard"];

    EXPECT_EQ(array_shape.size(), 5);
    EXPECT_EQ(array_shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(array_shape[1].get<int>(), array_channels);
    EXPECT_EQ(array_shape[2].get<int>(), array_planes);
    EXPECT_EQ(array_shape[3].get<int>(), array_height);
    EXPECT_EQ(array_shape[4].get<int>(), array_width);

    EXPECT_EQ(chunk_shape.size(), 5);
    EXPECT_EQ(chunk_shape[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(chunk_shape[1].get<int>(), chunk_channels);
    EXPECT_EQ(chunk_shape[2].get<int>(), chunk_planes);
    EXPECT_EQ(chunk_shape[3].get<int>(), chunk_height);
    EXPECT_EQ(chunk_shape[4].get<int>(), chunk_width);

    EXPECT_EQ(shard_shape.size(), 5);
    EXPECT_EQ(shard_shape[0].get<int>(), shard_timepoints);
    EXPECT_EQ(shard_shape[1].get<int>(), shard_channels);
    EXPECT_EQ(shard_shape[2].get<int>(), shard_planes);
    EXPECT_EQ(shard_shape[3].get<int>(), shard_height);
    EXPECT_EQ(shard_shape[4].get<int>(), shard_width);
}

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    int retval = 1;

    const ZarrDataType dtype = ZarrDataType_uint16;
    const unsigned int nbytes_px = zarr::bytes_of_type(dtype);

    try {
        auto thread_pool = std::make_shared<zarr::ThreadPool>(
          std::thread::hardware_concurrency(),
          [](const std::string& err) { LOG_ERROR("Error: %s", err.c_str()); });

        std::vector<zarr::Dimension> dims;
        dims.emplace_back("t",
                          ZarrDimensionType_Time,
                          array_timepoints,
                          chunk_timepoints,
                          shard_timepoints);
        dims.emplace_back("c",
                          ZarrDimensionType_Channel,
                          array_channels,
                          chunk_channels,
                          shard_channels);
        dims.emplace_back("z",
                          ZarrDimensionType_Space,
                          array_planes,
                          chunk_planes,
                          shard_planes);
        dims.emplace_back("y",
                          ZarrDimensionType_Space,
                          array_height,
                          chunk_height,
                          shard_height);
        dims.emplace_back(
          "x", ZarrDimensionType_Space, array_width, chunk_width, shard_width);

        zarr::ArrayWriterConfig config = {
            .dimensions = dims,
            .dtype = dtype,
            .level_of_detail = level_of_detail,
            .bucket_name = std::nullopt,
            .store_path = base_dir.string(),
            .compression_params = std::nullopt,
        };

        {
            zarr::ZarrV3ArrayWriter writer(config, thread_pool, nullptr);

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write_frame(data.data(), frame_size));
            }
        }

        check_json();

        const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                                chunk_channels * chunk_timepoints * nbytes_px;
        const auto index_size = chunks_per_shard *
                                sizeof(uint64_t) * // indices are 64 bits
                                2;                 // 2 indices per chunk
        const auto expected_file_size = shard_width * shard_height *
                                          shard_planes * shard_channels *
                                          shard_timepoints * chunk_size +
                                        index_size;

        const fs::path data_root =
          base_dir / "data/root" / std::to_string(config.level_of_detail);
        CHECK(fs::is_directory(data_root));
        for (auto t = 0; t < shards_in_t; ++t) {
            const auto t_dir = data_root / ("c" + std::to_string(t));
            CHECK(fs::is_directory(t_dir));

            for (auto c = 0; c < shards_in_c; ++c) {
                const auto c_dir = t_dir / std::to_string(c);
                CHECK(fs::is_directory(c_dir));

                for (auto z = 0; z < shards_in_z; ++z) {
                    const auto z_dir = c_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < shards_in_y; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < shards_in_x; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            EXPECT_EQ(file_size, expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(shards_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(shards_in_y)));
                }

                CHECK(!fs::is_directory(c_dir / std::to_string(shards_in_z)));
            }

            CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_c)));
        }

        CHECK(
          !fs::is_directory(data_root / ("c" + std::to_string(shards_in_t))));

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    // cleanup
    if (fs::exists(base_dir)) {
        fs::remove_all(base_dir);
    }
    return retval;
}