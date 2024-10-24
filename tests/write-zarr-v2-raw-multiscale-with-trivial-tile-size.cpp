/// @brief Test that an acquisition to Zarr with multiscale enabled and trivial
/// chunk sizes will downsample exactly one time.

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

#include "nlohmann/json.hpp"

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

/// Check that strings a == b
/// example: `ASSERT_STREQ("foo",container_of_foo)`
#define ASSERT_STREQ(a, b)                                                     \
    do {                                                                       \
        std::string a_ = (a);                                                  \
        std::string b_ = (b);                                                  \
        EXPECT(a_ == b_,                                                       \
               "Expected '%s'=='%s' but '%s'!= '%s'",                          \
               #a,                                                             \
               #b,                                                             \
               a_.c_str(),                                                     \
               b_.c_str());                                                    \
    } while (0)

const static uint32_t frame_width = 240;
const static uint32_t frame_height = 135;
const static uint32_t chunk_planes = 128;

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
                                SIZED("simulated.*empty.*"),
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
                                  sample_spacing_um,
                                  4));

    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           0,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           chunk_planes,
                                           0));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           1,
                                           SIZED("c") + 1,
                                           DimensionType_Channel,
                                           1,
                                           1,
                                           0));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           2,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           frame_height,
                                           frame_height,
                                           0));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           3,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           frame_width,
                                           frame_width,
                                           0));

    CHECK(storage_properties_set_enable_multiscale(
      &props.video[0].storage.settings, 1));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    props.video[0].max_frame_count = chunk_planes;

    OK(acquire_configure(runtime, &props));
    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));

    storage_properties_destroy(&props.video[0].storage.settings);
}

struct LayerTestCase
{
    int layer;
    int frame_width;
    int frame_height;
    int tile_width;
    int tile_height;
    int frames_per_layer;
    int frames_per_chunk;
};

void
verify_layer(const LayerTestCase& test_case)
{
    const auto layer = test_case.layer;
    const auto layer_tile_width = test_case.tile_width;
    const auto layer_frame_width = test_case.frame_width;
    const auto layer_tile_height = test_case.tile_height;
    const auto layer_frame_height = test_case.frame_height;
    const auto frames_per_layer = test_case.frames_per_layer;
    const auto frames_per_chunk = test_case.frames_per_chunk;

    const auto zarray_path =
      fs::path(TEST ".zarr") / std::to_string(layer) / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    CHECK(fs::file_size(zarray_path) > 0);

    // check metadata
    std::ifstream f(zarray_path);
    json zarray = json::parse(f);

    const std::string dtype =
      std::endian::native == std::endian::little ? "<u1" : ">u1";
    CHECK(dtype == zarray["dtype"].get<std::string>());

    const auto shape = zarray["shape"];
    ASSERT_EQ(int, "%d", frames_per_layer, shape[0]);
    ASSERT_EQ(int, "%d", 1, shape[1]);
    ASSERT_EQ(int, "%d", layer_frame_height, shape[2]);
    ASSERT_EQ(int, "%d", layer_frame_width, shape[3]);

    const auto chunks = zarray["chunks"];
    ASSERT_EQ(int, "%d", frames_per_chunk, chunks[0].get<int>());
    ASSERT_EQ(int, "%d", 1, chunks[1].get<int>());
    ASSERT_EQ(int, "%d", layer_tile_height, chunks[2].get<int>());
    ASSERT_EQ(int, "%d", layer_tile_width, chunks[3].get<int>());

    // check chunked data
    auto chunk_size = chunks[0].get<int>() * chunks[1].get<int>() *
                      chunks[2].get<int>() * chunks[3].get<int>();

    const auto tiles_in_x =
      (uint32_t)std::ceil((float)layer_frame_width / (float)layer_tile_width);
    const auto tiles_in_y =
      (uint32_t)std::ceil((float)layer_frame_height / (float)layer_tile_height);

    for (auto i = 0; i < tiles_in_y; ++i) {
        for (auto j = 0; j < tiles_in_x; ++j) {
            const auto chunk_file_path = fs::path(TEST ".zarr/") /
                                         std::to_string(layer) / "0" / "0" /
                                         std::to_string(i) / std::to_string(j);
            CHECK(fs::is_regular_file(chunk_file_path));
            const auto file_size = fs::file_size(chunk_file_path);
            ASSERT_EQ(int, "%d", chunk_size, file_size);
        }
    }

    // check there's not a second chunk in t
    auto missing_path = fs::path(TEST ".zarr/") / std::to_string(layer) / "1";
    CHECK(!fs::is_regular_file(missing_path));

    // check there's not a second chunk in z
    missing_path = fs::path(TEST ".zarr/") / std::to_string(layer) / "0" / "1";
    CHECK(!fs::is_regular_file(missing_path));

    // check there's no add'l chunks in y
    missing_path = fs::path(TEST ".zarr/") / std::to_string(layer) / "0" / "0" /
                   std::to_string(tiles_in_y);
    CHECK(!fs::is_regular_file(missing_path));

    // check there's no add'l chunks in y
    missing_path = fs::path(TEST ".zarr/") / std::to_string(layer) / "0" / "0" /
                   "0" / std::to_string(tiles_in_x);
    CHECK(!fs::is_regular_file(missing_path));
}

void
validate()
{
    CHECK(fs::is_directory(TEST ".zarr"));

    const auto external_metadata_path = fs::path(TEST ".zarr") / "acquire.json";
    CHECK(fs::is_regular_file(external_metadata_path));
    CHECK(fs::file_size(external_metadata_path) > 0);

    const auto group_zattrs_path = fs::path(TEST ".zarr") / ".zattrs";
    CHECK(fs::is_regular_file(group_zattrs_path));
    CHECK(fs::file_size(group_zattrs_path) > 0);

    // check metadata
    std::ifstream f(group_zattrs_path);
    json group_zattrs = json::parse(f);

    const auto multiscales = group_zattrs["multiscales"][0];

    const auto& axes = multiscales["axes"];
    ASSERT_EQ(int, "%d", 4, axes.size());

    ASSERT_STREQ("t", axes[0]["name"]);
    ASSERT_STREQ("time", axes[0]["type"]);

    ASSERT_STREQ("c", axes[1]["name"]);
    ASSERT_STREQ("channel", axes[1]["type"]);

    ASSERT_STREQ("y", axes[2]["name"]);
    ASSERT_STREQ("space", axes[2]["type"]);
    ASSERT_STREQ("micrometer", axes[2]["unit"]);

    ASSERT_STREQ("x", axes[3]["name"]);
    ASSERT_STREQ("space", axes[3]["type"]);
    ASSERT_STREQ("micrometer", axes[3]["unit"]);

    const auto& datasets = multiscales["datasets"];
    ASSERT_EQ(int, "%d", 2, datasets.size());
    for (auto i = 0; i < 2; ++i) {
        const auto& dataset = datasets.at(i);
        ASSERT_STREQ(std::to_string(i), dataset["path"]);

        const auto& coord_trans = dataset["coordinateTransformations"][0];
        ASSERT_STREQ("scale", coord_trans["type"]);

        const auto& scale = coord_trans["scale"];
        ASSERT_EQ(float, "%f", std::pow(2.f, i), scale[0].get<float>());
        ASSERT_EQ(float, "%f", 1.f, scale[1].get<float>());
        ASSERT_EQ(float, "%f", std::pow(2.f, i), scale[2].get<float>());
        ASSERT_EQ(float, "%f", std::pow(2.f, i), scale[3].get<float>());
    }

    ASSERT_STREQ(multiscales["type"], "local_mean");

    // verify each layer
    verify_layer({ 0, 240, 135, 240, 135, 128, 128 });
    verify_layer({ 1, 120, 68, 120, 68, 64, 128 });

    auto missing_path = fs::path(TEST ".zarr/2");
    CHECK(!fs::exists(missing_path));
}

int
main()
{
    int retval = 1;
    auto runtime = acquire_init(reporter);

    try {
        acquire(runtime, TEST ".zarr");
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
