/// @file metadata-dimension-sizes.cpp
/// @brief Test that the dimension sizes are correctly reported in the metadata
/// for both Zarr V2 and Zarr V3.

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

/// Check that a>b
/// example: `ASSERT_GT(int,"%d",43,meaning_of_life())`
#define ASSERT_GT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ > b_, "Expected (%s) > (%s) but " fmt "<=" fmt, #a, #b, a_, b_);  \
    } while (0)

const static uint32_t array_width = 1920;
const static uint32_t chunk_width = 960;

const static uint32_t array_height = 1080;
const static uint32_t chunk_height = 540;

const static uint32_t array_planes = 8;
const static uint32_t chunk_planes = 4;

const static uint32_t array_channels = 3;
const static uint32_t chunk_channels = 1;

const static uint32_t chunk_timepoints = 10;

using StreamCameraProperties =
  AcquireProperties::aq_properties_video_s::aq_properties_camera_s;
using StreamStorageProperties =
  AcquireProperties::aq_properties_video_s::aq_properties_storage_s;

void
configure_camera(AcquireRuntime* runtime, StreamCameraProperties& props)
{
    CHECK(runtime);
    const DeviceManager* dm = acquire_device_manager(runtime);
    CHECK(dm);

    DEVOK(device_manager_select(
      dm, DeviceKind_Camera, SIZED("simulated.*random.*"), &props.identifier));

    props.settings.binning = 1;
    props.settings.pixel_type = SampleType_u8;
    props.settings.shape = { .x = array_width, .y = array_height };
}

void
configure_storage(AcquireRuntime* runtime,
                  StreamStorageProperties& props,
                  const std::string& kind,
                  const std::string& uri)
{
    CHECK(runtime);
    const DeviceManager* dm = acquire_device_manager(runtime);
    CHECK(dm);

    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                kind.c_str(),
                                kind.size() + 1,
                                &props.identifier));

    storage_properties_init(
      &props.settings, 0, uri.c_str(), uri.size() + 1, nullptr, 0, { 1, 1 }, 5);

    CHECK(storage_properties_set_dimension(&props.settings,
                                           0,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           chunk_timepoints,
                                           1));
    CHECK(storage_properties_set_dimension(&props.settings,
                                           1,
                                           SIZED("c") + 1,
                                           DimensionType_Channel,
                                           array_channels,
                                           chunk_channels,
                                           1));
    CHECK(storage_properties_set_dimension(&props.settings,
                                           2,
                                           SIZED("z") + 1,
                                           DimensionType_Space,
                                           array_planes,
                                           chunk_planes,
                                           1));
    CHECK(storage_properties_set_dimension(&props.settings,
                                           3,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           array_height,
                                           chunk_height,
                                           1));
    CHECK(storage_properties_set_dimension(&props.settings,
                                           4,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           array_width,
                                           chunk_width,
                                           1));
}

void
configure(AcquireRuntime* runtime)
{
    CHECK(runtime);
    const DeviceManager* dm = acquire_device_manager(runtime);
    CHECK(dm);

    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    // camera
    configure_camera(runtime, props.video[0].camera);
    configure_camera(runtime, props.video[1].camera);

    // storage
    configure_storage(runtime, props.video[0].storage, "Zarr", TEST "-v2.zarr");
    configure_storage(
      runtime, props.video[1].storage, "ZarrV3", TEST "-v3.zarr");

    // acquisition
    props.video[0].max_frame_count =
      array_planes * array_channels * chunk_timepoints + 1;
    props.video[1].max_frame_count = props.video[0].max_frame_count;

    OK(acquire_configure(runtime, &props));
}

void
acquire(AcquireRuntime* runtime)
{
    acquire_start(runtime);
    acquire_stop(runtime);
}

void
validate_ome_metadata(const json& j)
{

    auto multiscales = j["multiscales"][0];
    auto axes = multiscales["axes"];

    ASSERT_EQ(int, "%d", 5, axes.size());
    ASSERT_STREQ("t", axes[0]["name"]);
    ASSERT_STREQ("time", axes[0]["type"]);
    ASSERT_STREQ("c", axes[1]["name"]);
    ASSERT_STREQ("channel", axes[1]["type"]);
    ASSERT_STREQ("z", axes[2]["name"]);
    ASSERT_STREQ("space", axes[2]["type"]);
    ASSERT_STREQ("y", axes[3]["name"]);
    ASSERT_STREQ("space", axes[3]["type"]);
    ASSERT_STREQ("micrometer", axes[3]["unit"]);
    ASSERT_STREQ("x", axes[4]["name"]);
    ASSERT_STREQ("space", axes[4]["type"]);
    ASSERT_STREQ("micrometer", axes[4]["unit"]);

    auto datasets = multiscales["datasets"][0];
    ASSERT_STREQ("0", datasets["path"]);

    auto transformations = datasets["coordinateTransformations"][0];
    ASSERT_STREQ("scale", transformations["type"]);

    auto scale = transformations["scale"];
    ASSERT_EQ(int, "%d", 5, scale.size());

    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(double, "%f", 1.0, scale[i]);
    }
}

void
validate_array_v2(const json& j)
{
    const uint32_t array_timepoints = chunk_timepoints + 1;

    ASSERT_EQ(int, "%d", 5, j["shape"].size());
    ASSERT_EQ(int, "%d", array_timepoints, j["shape"][0]);
    ASSERT_EQ(int, "%d", array_channels, j["shape"][1]);
    ASSERT_EQ(int, "%d", array_planes, j["shape"][2]);
    ASSERT_EQ(int, "%d", array_height, j["shape"][3]);
    ASSERT_EQ(int, "%d", array_width, j["shape"][4]);

    ASSERT_EQ(int, "%d", 5, j["chunks"].size());
    ASSERT_EQ(int, "%d", chunk_timepoints, j["chunks"][0]);
    ASSERT_EQ(int, "%d", chunk_channels, j["chunks"][1]);
    ASSERT_EQ(int, "%d", chunk_planes, j["chunks"][2]);
    ASSERT_EQ(int, "%d", chunk_height, j["chunks"][3]);
    ASSERT_EQ(int, "%d", chunk_width, j["chunks"][4]);
}

void
validate_array_v3(const json& j)
{
    const uint32_t array_timepoints = chunk_timepoints + 1;

    ASSERT_EQ(int, "%d", 5, j["shape"].size());
    ASSERT_EQ(int, "%d", array_timepoints, j["shape"][0]);
    ASSERT_EQ(int, "%d", array_channels, j["shape"][1]);
    ASSERT_EQ(int, "%d", array_planes, j["shape"][2]);
    ASSERT_EQ(int, "%d", array_height, j["shape"][3]);
    ASSERT_EQ(int, "%d", array_width, j["shape"][4]);

    const auto chunk_shape = j["chunk_grid"]["configuration"]["chunk_shape"];
    ASSERT_EQ(int, "%d", 5, chunk_shape.size());
    ASSERT_EQ(int, "%d", chunk_timepoints, chunk_shape[0]);
    ASSERT_EQ(int, "%d", chunk_channels, chunk_shape[1]);
    ASSERT_EQ(int, "%d", chunk_planes, chunk_shape[2]);
    ASSERT_EQ(int, "%d", chunk_height, chunk_shape[3]);
    ASSERT_EQ(int, "%d", chunk_width, chunk_shape[4]);
}

void
validate(AcquireRuntime* runtime)
{
    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    const auto stream0_path = fs::path(props.video[0].storage.settings.uri.str + 7);
    CHECK(fs::is_directory(stream0_path));

    // OME metadata in Zarr V2
    const fs::path group_zattrs_path = stream0_path / ".zattrs";
    {
        CHECK(fs::is_regular_file(group_zattrs_path));

        std::ifstream ifs(group_zattrs_path);
        json j;
        ifs >> j;
        validate_ome_metadata(j);
    }

    // Array metadata in Zarr V2
    const fs::path zarray_path = stream0_path / "0" / ".zarray";
    {
        CHECK(fs::is_regular_file(zarray_path));

        std::ifstream ifs(zarray_path);
        json j;
        ifs >> j;
        validate_array_v2(j);
    }

    const auto stream1_path = fs::path(props.video[1].storage.settings.uri.str + 7);
    CHECK(fs::is_directory(stream1_path));

    // OME metadata in Zarr V3
    const fs::path group_metadata_path = stream1_path / "zarr.json";
    {
        CHECK(fs::is_regular_file(group_metadata_path));

        std::ifstream ifs(group_metadata_path);
        json j;
        ifs >> j;

        validate_ome_metadata(j["attributes"]);
    }

    // Array metadata in Zarr V3
    const fs::path array_metadata_path = stream1_path / "0" / "zarr.json";
    {
        CHECK(fs::is_regular_file(array_metadata_path));

        std::ifstream ifs(array_metadata_path);
        json j;
        ifs >> j;

        validate_array_v3(j);
    }
}

int
main()
{
    int retval = 1;
    AcquireRuntime* runtime = acquire_init(reporter);

    try {
        configure(runtime);
        acquire(runtime);
        validate(runtime);

        retval = 0;
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    acquire_shutdown(runtime);
    return retval;
}
