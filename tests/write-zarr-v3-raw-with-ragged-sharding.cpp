/// @brief Test that we can write ragged shards with the ZarrV3 writer.

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "platform.h" // clock
#include "logger.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "json.hpp"

namespace fs = std::filesystem;
using json = nlohmann::json;

void
reporter(int is_error,
         const char* file,
         int line,
         const char* function,
         const char* msg)
{
    fprintf(is_error ? stderr : stdout,
            "%s%s(%d) - %s: %s\n",
            is_error ? "ERROR " : "",
            file,
            line,
            function,
            msg);
}

/// Helper for passing size static strings as function args.
/// For a function: `f(char*,size_t)` use `f(SIZED("hello"))`.
/// Expands to `f("hello",5)`.
#define SIZED(str) str, sizeof(str) - 1

#define L (aq_logger)
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define ERR(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            char buf[1 << 8] = { 0 };                                          \
            ERR(__VA_ARGS__);                                                  \
            snprintf(buf, sizeof(buf) - 1, __VA_ARGS__);                       \
            throw std::runtime_error(buf);                                     \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false: %s", #e)
#define DEVOK(e) CHECK(Device_Ok == (e))
#define OK(e) CHECK(AcquireStatus_Ok == (e))

/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
    } while (0)

/// Check that a>b
/// example: `ASSERT_GT(int,"%d",43,meaning_of_life())`
#define ASSERT_GT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ > b_, "Expected (%s) > (%s) but " fmt "<=" fmt, #a, #b, a_, b_);  \
    } while (0)

const static uint32_t frame_width = 1920;
const static uint32_t chunk_width = frame_width / 7; // ragged
const static uint32_t shard_width = 6; // 6 chunks per shard, ragged

const static uint32_t frame_height = 1080;
const static uint32_t chunk_height = frame_height / 7; // ragged
const static uint32_t shard_height = 7; // 7 chunks per shard, ragged

const static uint32_t frames_per_chunk = 16;
const static uint32_t max_frame_count = 16;

void
setup(AcquireRuntime* runtime)
{
    const char* filename = TEST ".zarr";
    auto dm = acquire_device_manager(runtime);
    CHECK(runtime);
    CHECK(dm);

    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    DEVOK(device_manager_select(dm,
                                DeviceKind_Camera,
                                SIZED("simulated.*radial.*"),
                                &props.video[0].camera.identifier));
    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("ZarrV3"),
                                &props.video[0].storage.identifier));

    const struct PixelScale sample_spacing_um = { 1, 1 };

    CHECK(storage_properties_init(&props.video[0].storage.settings,
                                  0,
                                  (char*)filename,
                                  strlen(filename) + 1,
                                  nullptr,
                                  0,
                                  sample_spacing_um,
                                  3));

    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           0,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           frame_width,
                                           chunk_width,
                                           shard_width));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           1,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           frame_height,
                                           chunk_height,
                                           shard_height));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           2,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           frames_per_chunk,
                                           1));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    props.video[0].max_frame_count = max_frame_count;
    props.video[0].camera.settings.exposure_time_us = 5e5;

    OK(acquire_configure(runtime, &props));

    storage_properties_destroy(&props.video[0].storage.settings);
}

void
acquire(AcquireRuntime* runtime)
{
    const auto next = [](VideoFrame* cur) -> VideoFrame* {
        return (VideoFrame*)(((uint8_t*)cur) + cur->bytes_of_frame);
    };

    const auto consumed_bytes = [](const VideoFrame* const cur,
                                   const VideoFrame* const end) -> size_t {
        return (uint8_t*)end - (uint8_t*)cur;
    };

    AcquireProperties props = { 0 };
    OK(acquire_get_configuration(runtime, &props));

    struct clock clock;
    static double time_limit_ms =
      2 * max_frame_count * props.video[0].camera.settings.exposure_time_us /
      1000.;
    clock_init(&clock);
    clock_shift_ms(&clock, time_limit_ms);
    OK(acquire_start(runtime));
    {
        uint64_t nframes = 0;
        VideoFrame *beg, *end, *cur;
        do {
            struct clock throttle;
            clock_init(&throttle);
            EXPECT(clock_cmp_now(&clock) < 0,
                   "Timeout at %f ms",
                   clock_toc_ms(&clock) + time_limit_ms);
            OK(acquire_map_read(runtime, 0, &beg, &end));
            for (cur = beg; cur < end; cur = next(cur)) {
                LOG("stream %d counting frame w id %d", 0, cur->frame_id);
                CHECK(cur->shape.dims.width == frame_width);
                CHECK(cur->shape.dims.height == frame_height);
                ++nframes;
            }
            {
                uint32_t n = consumed_bytes(beg, end);
                OK(acquire_unmap_read(runtime, 0, n));
                if (n)
                    LOG("stream %d consumed bytes %d", 0, n);
            }
            clock_sleep_ms(&throttle, 100.0f);

            LOG(
              "stream %d nframes %d time %f", 0, nframes, clock_toc_ms(&clock));
        } while (DeviceState_Running == acquire_get_state(runtime) &&
                 nframes < max_frame_count);

        OK(acquire_map_read(runtime, 0, &beg, &end));
        for (cur = beg; cur < end; cur = next(cur)) {
            LOG("stream %d counting frame w id %d", 0, cur->frame_id);
            CHECK(cur->shape.dims.width == frame_width);
            CHECK(cur->shape.dims.height == frame_height);
            ++nframes;
        }
        {
            uint32_t n = consumed_bytes(beg, end);
            OK(acquire_unmap_read(runtime, 0, n));
            if (n)
                LOG("stream %d consumed bytes %d", 0, n);
        }

        CHECK(nframes == max_frame_count);
    }

    OK(acquire_stop(runtime));
}

void
validate()
{
    const fs::path test_path(TEST ".zarr");
    CHECK(fs::is_directory(test_path));

    // check the zarr.json metadata file
    fs::path metadata_path = test_path / "zarr.json";
    CHECK(fs::is_regular_file(metadata_path));
    std::ifstream f(metadata_path);
    json metadata = json::parse(f);

    CHECK(metadata["extensions"].empty());
    CHECK("https://purl.org/zarr/spec/protocol/core/3.0" ==
          metadata["metadata_encoding"]);
    CHECK(".json" == metadata["metadata_key_suffix"]);
    CHECK("https://purl.org/zarr/spec/protocol/core/3.0" ==
          metadata["zarr_format"]);

    // check the group metadata file
    metadata_path = test_path / "meta" / "root.group.json";
    CHECK(fs::is_regular_file(metadata_path));

    f = std::ifstream(metadata_path);
    metadata = json::parse(f);
    CHECK("" == metadata["attributes"]["acquire"]);

    // check the array metadata file
    metadata_path = test_path / "meta" / "root" / "0.array.json";
    CHECK(fs::is_regular_file(metadata_path));

    f = std::ifstream(metadata_path);
    metadata = json::parse(f);

    const auto chunk_grid = metadata["chunk_grid"];
    CHECK("/" == chunk_grid["separator"]);
    CHECK("regular" == chunk_grid["type"]);

    const auto chunk_shape = chunk_grid["chunk_shape"];
    ASSERT_EQ(int, "%d", frames_per_chunk, chunk_shape[0]);
    ASSERT_EQ(int, "%d", chunk_height, chunk_shape[1]);
    ASSERT_EQ(int, "%d", chunk_width, chunk_shape[2]);

    CHECK("C" == metadata["chunk_memory_layout"]);
    CHECK("u1" == metadata["data_type"]);
    CHECK(metadata["extensions"].empty());

    const auto array_shape = metadata["shape"];
    ASSERT_EQ(int, "%d", max_frame_count, array_shape[0]);
    ASSERT_EQ(int, "%d", frame_height, array_shape[1]);
    ASSERT_EQ(int, "%d", frame_width, array_shape[2]);

    // sharding
    const auto storage_transformers = metadata["storage_transformers"];
    const auto configuration = storage_transformers[0]["configuration"];
    const auto& cps = configuration["chunks_per_shard"];
    ASSERT_EQ(int, "%d", 1, cps[0]);
    ASSERT_EQ(int, "%d", shard_height, cps[1]);
    ASSERT_EQ(int, "%d", shard_width, cps[2]);

    const size_t chunks_per_full_shard =
      cps[0].get<size_t>() * cps[1].get<size_t>() * cps[2].get<size_t>();
    ASSERT_EQ(
      int, "%d", shard_width* shard_height, chunks_per_full_shard); // 42 chunks

    const size_t index_size = 2 * chunks_per_full_shard * sizeof(uint64_t);

    const uint32_t bytes_per_chunk = chunk_shape[0].get<uint32_t>() *
                                     chunk_shape[1].get<uint32_t>() *
                                     chunk_shape[2].get<uint32_t>();

    // 1st shard is full
    {
        const fs::path shard_path =
          test_path / "data" / "root" / "0" / "c0" / "0" / "0";
        CHECK(fs::is_regular_file(shard_path));

        const int expected_file_size =
          chunks_per_full_shard * bytes_per_chunk + // 42 chunks
          index_size;                               // 42 indices
        const int file_size = fs::file_size(shard_path);
        ASSERT_EQ(int, "%d", expected_file_size, file_size);

        // check the indices at the end of the file
        std::ifstream shard_file(shard_path, std::ios::binary);
        shard_file.seekg(-index_size, std::ios::end);
        std::vector<uint64_t> indices(2 * chunks_per_full_shard);
        shard_file.read(reinterpret_cast<char*>(indices.data()), index_size);

        for (auto i = 0; i < indices.size(); i += 2) {
            ASSERT_EQ(
              uint64_t, "%llu", (i / 2) * bytes_per_chunk, indices.at(i));
        }
        for (auto i = 1; i < indices.size(); i += 2) {
            ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(i));
        }
    }

    // 2nd shard has 14 chunks, arranged 7 x 2
    {
        const size_t chunks_this_shard = 14;

        const fs::path shard_path =
          test_path / "data" / "root" / "0" / "c0" / "0" / "1";
        CHECK(fs::is_regular_file(shard_path));

        const int expected_file_size =
          chunks_this_shard * bytes_per_chunk + // 14 chunks
          index_size;                           // 42 indices
        const int file_size = fs::file_size(shard_path);
        ASSERT_EQ(int, "%d", expected_file_size, file_size);

        // check the indices at the end of the file
        std::ifstream shard_file(shard_path, std::ios::binary);
        shard_file.seekg(-index_size, std::ios::end);
        std::vector<uint64_t> indices(2 * chunks_per_full_shard);
        shard_file.read(reinterpret_cast<char*>(indices.data()), index_size);

        size_t offset = 0;
        for (auto i = 0; i < indices.size(); i += 12) {
            ASSERT_EQ(uint64_t, "%llu", offset, indices.at(i));
            ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(i + 1));
            ASSERT_EQ(
              uint64_t, "%llu", offset + bytes_per_chunk, indices.at(i + 2));
            ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(i + 3));

            // the rest of the indices should be empty
            for (auto j = 4; j < 12; ++j) {
                ASSERT_EQ(uint64_t,
                          "%llu",
                          std::numeric_limits<uint64_t>::max(),
                          indices.at(i + j));
            }

            offset += 2 * bytes_per_chunk;
        }
    }

    // 3rd shard has 6 chunks, arranged horizontally
    {
        const size_t chunks_this_shard = 6;

        const fs::path shard_path =
          test_path / "data" / "root" / "0" / "c0" / "1" / "0";
        CHECK(fs::is_regular_file(shard_path));

        const int expected_file_size =
          chunks_this_shard * bytes_per_chunk + // 6 chunks
          index_size;                           // 42 indices
        const int file_size = fs::file_size(shard_path);
        ASSERT_EQ(int, "%d", expected_file_size, file_size);

        // check the indices at the end of the file
        std::ifstream shard_file(shard_path, std::ios::binary);
        shard_file.seekg(-index_size, std::ios::end);
        std::vector<uint64_t> indices(2 * chunks_per_full_shard);
        shard_file.read(reinterpret_cast<char*>(indices.data()), index_size);

        for (auto i = 0; i < 12; i += 2) {
            ASSERT_EQ(
              uint64_t, "%llu", (i / 2) * bytes_per_chunk, indices.at(i));
            ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(i + 1));
        }

        // the rest of the indices should be empty
        for (auto i = 12; i < indices.size(); ++i) {
            ASSERT_EQ(uint64_t,
                      "%llu",
                      std::numeric_limits<uint64_t>::max(),
                      indices.at(i));
        }
    }

    // 4th shard has 2 chunks, arranged horizontally
    {
        const size_t chunks_this_shard = 2;

        const fs::path shard_path =
          test_path / "data" / "root" / "0" / "c0" / "1" / "1";
        CHECK(fs::is_regular_file(shard_path));

        const int expected_file_size =
          chunks_this_shard * bytes_per_chunk + // 2 chunks
          index_size;                           // 42 indices
        const int file_size = fs::file_size(shard_path);
        ASSERT_EQ(int, "%d", expected_file_size, file_size);

        // check the indices at the end of the file
        std::ifstream shard_file(shard_path, std::ios::binary);
        shard_file.seekg(-index_size, std::ios::end);
        std::vector<uint64_t> indices(2 * chunks_per_full_shard);
        shard_file.read(reinterpret_cast<char*>(indices.data()), index_size);

        ASSERT_EQ(uint64_t, "%llu", 0, indices.at(0));
        ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(1));
        ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(2));
        ASSERT_EQ(uint64_t, "%llu", bytes_per_chunk, indices.at(3));

        for (auto i = 4; i < indices.size(); ++i) {
            ASSERT_EQ(uint64_t,
                      "%llu",
                      std::numeric_limits<uint64_t>::max(),
                      indices.at(i));
        }
    }
}

int
main()
{
    int retval = 1;
    auto runtime = acquire_init(reporter);

    try {
        setup(runtime);
        acquire(runtime);
        validate();

        retval = 0;
        LOG("Done (OK)");
    } catch (const std::exception& exc) {
        ERR("Exception: %s", exc.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    acquire_shutdown(runtime);

    return retval;
}
