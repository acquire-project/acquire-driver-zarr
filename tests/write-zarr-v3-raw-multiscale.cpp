/// @brief Test that an acquisition to Zarr V3 with multiscale enabled writes
/// multiple layers to the Zarr group, that the layers are the correct size,
/// that they are chunked accordingly, and that the metadata is written
/// correctly.

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

const static uint32_t chunk_width = frame_width / 3;
const static uint32_t chunk_height = frame_height / 3;
const static uint32_t chunk_planes = 128;

const static uint64_t max_frames = 100;

void
configure(AcquireRuntime* runtime)
{
    CHECK(runtime);

    const DeviceManager* dm = acquire_device_manager(runtime);
    CHECK(dm);

    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    // configure camera
    DEVOK(device_manager_select(dm,
                                DeviceKind_Camera,
                                SIZED("simulated.*empty.*"),
                                &props.video[0].camera.identifier));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };

    // configure storage
    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("ZarrV3"),
                                &props.video[0].storage.identifier));

    const char external_metadata[] = R"({"hello":"world"})";
    const struct PixelScale sample_spacing_um = { 1, 1 };

    std::string filename = TEST ".zarr";
    CHECK(storage_properties_init(&props.video[0].storage.settings,
                                  0,
                                  filename.c_str(),
                                  filename.size() + 1,
                                  (char*)external_metadata,
                                  sizeof(external_metadata),
                                  sample_spacing_um,
                                  4));

    CHECK(storage_properties_set_enable_multiscale(
      &props.video[0].storage.settings, 1));

    // configure storage dimensions
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           0,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           chunk_planes,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           1,
                                           SIZED("c") + 1,
                                           DimensionType_Channel,
                                           1,
                                           1,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           2,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           frame_height,
                                           chunk_height,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           3,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           frame_width,
                                           chunk_width,
                                           1));

    // configure acquisition
    props.video[0].max_frame_count = max_frames;

    OK(acquire_configure(runtime, &props));

    storage_properties_destroy(&props.video[0].storage.settings);
}

void
acquire(AcquireRuntime* runtime)
{
    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));
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

    const auto array_meta_path = fs::path(TEST ".zarr") / std::to_string(layer) / "zarr.json";
    CHECK(fs::is_regular_file(array_meta_path));
    CHECK(fs::file_size(array_meta_path) > 0);

    // check metadata
    std::ifstream f(array_meta_path);
    json array_meta = json::parse(f);

    const auto shape = array_meta["shape"];
    ASSERT_EQ(int, "%d", frames_per_layer, shape[0]);
    ASSERT_EQ(int, "%d", 1, shape[1]);
    ASSERT_EQ(int, "%d", layer_frame_height, shape[2]);
    ASSERT_EQ(int, "%d", layer_frame_width, shape[3]);

    const auto chunk_grid = array_meta["chunk_grid"];
    CHECK("regular" == chunk_grid["name"]);

    const auto chunk_shape = chunk_grid["configuration"]["chunk_shape"];
    ASSERT_EQ(int, "%d", frames_per_chunk, chunk_shape[0]);
    ASSERT_EQ(int, "%d", 1, chunk_shape[1]);
    ASSERT_EQ(int, "%d", layer_tile_height, chunk_shape[2]);
    ASSERT_EQ(int, "%d", layer_tile_width, chunk_shape[3]);

    const auto chunk_key_encoding = array_meta["chunk_key_encoding"];
    CHECK("/" == chunk_key_encoding["configuration"]["separator"]);

    // check chunked data
    size_t chunk_size = chunk_shape[0].get<int>() * chunk_shape[1].get<int>() *
                        chunk_shape[2].get<int>() * chunk_shape[3].get<int>();
    const size_t index_size = 2 * sizeof(uint64_t);
    const auto checksum_size = sizeof(uint32_t);
    const size_t shard_file_size = chunk_size + index_size + checksum_size;

    const auto shards_in_x =
      (layer_frame_width + layer_tile_width - 1) / layer_tile_width;

    const auto shards_in_y =
      (layer_frame_height + layer_tile_height - 1) / layer_tile_height;

    const fs::path layer_root = fs::path(TEST ".zarr") / std::to_string(layer);
    CHECK(fs::is_directory(layer_root));

    const fs::path t_path = layer_root / "c" / "0";
    CHECK(fs::is_directory(t_path));

    const fs::path c_path = t_path / "0";
    CHECK(fs::is_directory(c_path));

    for (auto i = 0; i < shards_in_y; ++i) {
        const fs::path y_path = c_path / std::to_string(i);
        CHECK(fs::is_directory(y_path));

        for (auto j = 0; j < shards_in_x; ++j) {
            const auto shard_file_path = y_path / std::to_string(j);
            CHECK(fs::is_regular_file(shard_file_path));
            ASSERT_EQ(int, "%d", shard_file_size, fs::file_size(shard_file_path));
        }
    }

    // check there's not a second shard in t
    auto missing_path = layer_root / "c" / "1";
    CHECK(!fs::is_regular_file(missing_path));

    // check there's not a second shard in c
    missing_path = t_path / "1";
    CHECK(!fs::is_regular_file(missing_path));

    // check there's no add'l shards in y
    missing_path = c_path / std::to_string(shards_in_y);
    CHECK(!fs::is_regular_file(missing_path));

    // check there's no add'l shards in x
    missing_path = c_path / "0" / std::to_string(shards_in_x);
    CHECK(!fs::is_regular_file(missing_path));
}

void
validate()
{
    CHECK(fs::is_directory(TEST ".zarr"));

    const auto group_meta_path = fs::path(TEST ".zarr") / "zarr.json";
    CHECK(fs::is_regular_file(group_meta_path));
    CHECK(fs::file_size(group_meta_path) > 0);

    // check metadata
    std::ifstream f(group_meta_path);
    json group_meta = json::parse(f);

    CHECK(group_meta["zarr_format"].get<int>() == 3);

    const auto multiscales = group_meta["attributes"]["multiscales"][0];

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
    ASSERT_EQ(int, "%d", 3, datasets.size());
    for (auto i = 0; i < 3; ++i) {
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
    verify_layer({ 0, 240, 135, 80, 45, 100, 128 });
    verify_layer({ 1, 120, 68, 80, 45, 50, 128 }); // padding here
    verify_layer({ 2, 60, 34, 60, 34, 25, 128 });

    auto missing_path = fs::path(TEST ".zarr/3");
    CHECK(!fs::exists(missing_path));
}

int
main()
{
    int retval = 1;
    auto runtime = acquire_init(reporter);

    try {
        configure(runtime);
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
