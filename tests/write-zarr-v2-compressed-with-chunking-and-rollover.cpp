#include <filesystem>
#include <fstream>
#include <string>
#include <stdexcept>

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "platform.h"
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
}

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

/// Check that a>b
/// example: `ASSERT_GT(int,"%d",43,meaning_of_life())`
#define ASSERT_GT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ > b_, "Expected (%s) > (%s) but " fmt "<=" fmt, #a, #b, a_, b_);  \
    } while (0)

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

const static uint32_t chunk_width = frame_width / 2;
const static uint32_t chunk_height = frame_height / 2;
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

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 1e4;
    // should trigger rollover
    props.video[0].max_frame_count = chunk_planes + 1;

    OK(acquire_configure(runtime, &props));
    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));
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

    const auto zarray_path = fs::path(TEST ".zarr") / "0" / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    CHECK(fs::file_size(zarray_path) > 0);

    // check metadata
    std::ifstream f(zarray_path);
    json zarray = json::parse(f);

    auto shape = zarray["shape"];
    ASSERT_EQ(int, "%d", chunk_planes + 1, shape[0]);
    ASSERT_EQ(int, "%d", 1, shape[1]);
    ASSERT_EQ(int, "%d", frame_height, shape[2]);
    ASSERT_EQ(int, "%d", frame_width, shape[3]);

    auto chunks = zarray["chunks"];
    ASSERT_EQ(int, "%d", chunk_planes, chunks[0]);
    ASSERT_EQ(int, "%d", 1, chunks[1]);
    ASSERT_EQ(int, "%d", chunk_height, chunks[2]);
    ASSERT_EQ(int, "%d", chunk_width, chunks[3]);

    // check chunked data
    auto chunk_size = chunks[0].get<int>() * chunks[1].get<int>() *
                      chunks[2].get<int>() * chunks[3].get<int>();

    auto chunk_file_path = fs::path(TEST ".zarr/0/0/0/0/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/0/0/0/1");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/0/0/1/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/0/0/1/1");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/1/0/0/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/1/0/0/1");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/1/0/1/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);

    chunk_file_path = fs::path(TEST ".zarr/0/1/0/1/1");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_GT(int, "%d", chunk_size, fs::file_size(chunk_file_path));
    ASSERT_GT(int, "%d", fs::file_size(chunk_file_path), 0);
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
