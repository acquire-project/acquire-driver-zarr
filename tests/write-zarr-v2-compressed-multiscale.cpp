/// @brief Test that an acquisition to compressed Zarr with multiscale enabled
/// writes multiple layers to the Zarr group, that the layers are the correct
/// size, that they are chunked accordingly, and that the metadata is written
/// correctly.

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

#include "json.hpp"

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {
void
init_array(struct StorageDimension** data, size_t size)
{
    if (!*data) {
        *data = new struct StorageDimension[size];
    }
}

void
destroy_array(struct StorageDimension* data)
{
    delete[] data;
}
} // end ::{anonymous} namespace

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

/// Check that a>b
/// example: `ASSERT_GT(int,"%d",43,meaning_of_life())`
#define ASSERT_GT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ > b_, "Expected (%s) > (%s) but " fmt "<=" fmt, #a, #b, a_, b_);  \
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

const static uint32_t frame_width = 1920;
const static uint32_t frame_height = 1080;

const static uint32_t chunk_width = frame_width / 3;
const static uint32_t chunk_height = frame_height / 3;
const static uint32_t chunk_planes = 72;

const static auto max_frames = 74;

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
                                SIZED("ZarrBlosc1ZstdByteShuffle"),
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

    props.video[0].storage.settings.acquisition_dimensions.init = init_array;
    props.video[0].storage.settings.acquisition_dimensions.destroy =
      destroy_array;

    CHECK(
      storage_properties_dimensions_init(&props.video[0].storage.settings, 4));
    auto* acq_dims = &props.video[0].storage.settings.acquisition_dimensions;

    CHECK(storage_dimension_init(acq_dims->data,
                                 SIZED("x") + 1,
                                 DimensionType_Space,
                                 frame_width,
                                 chunk_width,
                                 0));
    CHECK(storage_dimension_init(acq_dims->data + 1,
                                 SIZED("y") + 1,
                                 DimensionType_Space,
                                 frame_height,
                                 chunk_height,
                                 0));
    CHECK(storage_dimension_init(
      acq_dims->data + 2, SIZED("c") + 1, DimensionType_Channel, 1, 1, 0));
    CHECK(storage_dimension_init(acq_dims->data + 3,
                                 SIZED("t") + 1,
                                 DimensionType_Time,
                                 0,
                                 chunk_planes,
                                 0));

    CHECK(storage_properties_set_enable_multiscale(
      &props.video[0].storage.settings, 1));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    props.video[0].max_frame_count = max_frames;

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
            ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);
            ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
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

    const auto multiscales = group_zattrs["multiscales"][0];
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
    verify_layer({ 0, 1920, 1080, 640, 360, 74, 72 });
    verify_layer({ 1, 960, 540, 640, 360, 37, 72 });
    // rollover doesn't happen here since tile size is less than the specified
    // tile size
    verify_layer({ 2, 480, 270, 480, 270, 18, 72 });

    auto missing_path = fs::path(TEST ".zarr/3");
    CHECK(!fs::exists(missing_path));
}

int
main()
{
    int retval = 1;
    AcquireRuntime* runtime = acquire_init(reporter);

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
