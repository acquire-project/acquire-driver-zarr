#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

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

/// Check that a==b
/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
    } while (0)

const static uint32_t frame_width = 1920;
const static uint32_t frame_height = 1080;

const static uint32_t tile_width = frame_width / 1;
const static uint32_t tile_height = frame_height / 1;

const static uint32_t max_bytes_per_chunk = 16 << 20;
const static auto expected_frames_per_chunk = 8;
const static auto max_frames = 10;

const static int16_t max_layers = 2;
const static uint8_t downscale = 2;

void
acquire(AcquireRuntime* runtime, const char* filename)
{
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
                                SIZED("Zarr"),
                                &props.video[0].storage.identifier));

    const char external_metadata[] = R"({"hello":"world"})";
    const struct PixelScale sample_spacing_um = { 1, 1 };

    CHECK(storage_properties_init(&props.video[0].storage.settings,
                                  0,
                                  (char*)filename,
                                  strlen(filename) + 1,
                                  (char*)external_metadata,
                                  sizeof(external_metadata),
                                  sample_spacing_um));

    CHECK(
      storage_properties_set_chunking_props(&props.video[0].storage.settings,
                                            tile_width,
                                            tile_height,
                                            1,
                                            max_bytes_per_chunk));

    CHECK(
      storage_properties_set_multiscale_props(&props.video[0].storage.settings,
                                              SIZED("pyramid_box"),
                                              max_layers,
                                              downscale));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 1e5;
    props.video[0].max_frame_count = max_frames;

    OK(acquire_configure(runtime, &props));
    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));
}

void
verify_layer(int layer)
{
    const auto zarray_path =
      fs::path(TEST ".zarr") / std::to_string(layer) / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    CHECK(fs::file_size(zarray_path) > 0);

    // check metadata
    std::ifstream f(zarray_path);
    json zarray = json::parse(f);

    const auto chunks = zarray["chunks"];

    // check chunked data
    auto chunk_size = chunks[0].get<int>() * chunks[1].get<int>() *
                      chunks[2].get<int>() * chunks[3].get<int>();

    auto chunk_file_path =
      fs::path(TEST ".zarr/") / std::to_string(layer) / "0" / "0" / "0" / "0";
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));

    // check that there isn't a second (empty) chunk along the time dimension
    auto second_time_chunk_path =
      fs::path(TEST ".zarr") / std::to_string(layer) / "1";
    CHECK(!fs::exists(second_time_chunk_path));
}

int
main()
{
    auto runtime = acquire_init(reporter);
    acquire(runtime, TEST ".zarr");

    CHECK(fs::is_directory(TEST ".zarr"));

    const auto external_metadata_path =
      fs::path(TEST ".zarr") / "0" / ".zattrs";
    CHECK(fs::is_regular_file(external_metadata_path));
    CHECK(fs::file_size(external_metadata_path) > 0);

    const auto group_zattrs_path = fs::path(TEST ".zarr") / ".zattrs";
    CHECK(fs::is_regular_file(group_zattrs_path));
    CHECK(fs::file_size(group_zattrs_path) > 0);

    // check metadata
    std::ifstream f(group_zattrs_path);
    json group_zattrs = json::parse(f);

    auto datasets = group_zattrs["multiscales"][0]["datasets"];
    //    ASSERT_EQ(int, "%d", max_layers, datasets.size());

    const auto zarray_path = fs::path(TEST ".zarr") / "0" / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    CHECK(fs::file_size(zarray_path) > 0);

    // check metadata
    f = std::ifstream{ zarray_path };
    json zarray = json::parse(f);

    auto shape = zarray["shape"];
    ASSERT_EQ(int, "%d", max_frames, shape[0]);
    ASSERT_EQ(int, "%d", 1, shape[1]);
    ASSERT_EQ(int, "%d", frame_height, shape[2]);
    ASSERT_EQ(int, "%d", frame_width, shape[3]);

    auto chunks = zarray["chunks"];
    ASSERT_EQ(int, "%d", expected_frames_per_chunk, chunks[0]);
    ASSERT_EQ(int, "%d", 1, chunks[1]);
    ASSERT_EQ(int, "%d", tile_height, chunks[2]);
    ASSERT_EQ(int, "%d", tile_width, chunks[3]);

    // check chunked data
    auto chunk_size = chunks[0].get<int>() * chunks[1].get<int>() *
                      chunks[2].get<int>() * chunks[3].get<int>();

    auto chunk_file_path = fs::path(TEST ".zarr/0/0/0/0/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));

//    chunk_file_path = fs::path(TEST ".zarr/0/0/0/0/1");
//    CHECK(fs::is_regular_file(chunk_file_path));
//    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));
//
//    chunk_file_path = fs::path(TEST ".zarr/0/0/0/1/0");
//    CHECK(fs::is_regular_file(chunk_file_path));
//    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));
//
//    chunk_file_path = fs::path(TEST ".zarr/0/0/0/1/1");
//    CHECK(fs::is_regular_file(chunk_file_path));
//    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));
//
//    for (auto i = 0; i < datasets.size(); ++i) {
//        verify_layer(i);
//    }

    LOG("Done (OK)");
    acquire_shutdown(runtime);
    return 0;
}