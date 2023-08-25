// @file TODO: delete this file

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

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

const static uint32_t frame_width = 1200;
const static uint32_t frame_height = 1080;
const static uint32_t max_frames = 5000;

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

    storage_properties_init(&props.video[0].storage.settings,
                            0,
                            (char*)filename,
                            strlen(filename) + 1,
                            (char*)external_metadata,
                            sizeof(external_metadata),
                            sample_spacing_um);

    storage_properties_set_chunking_props(&props.video[0].storage.settings,
                                          frame_width / 3,
                                          frame_height / 2,
                                          1,
                                          64 << 20);

    storage_properties_set_enable_multiscale(&props.video[0].storage.settings,
                                             1);

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u16;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 1e4;
    props.video[0].max_frame_count = max_frames;

    OK(acquire_configure(runtime, &props));

    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));
}

int
main()
{
    auto runtime = acquire_init(reporter);
    acquire(runtime, TEST ".zarr");

    LOG("Done (OK)");
    acquire_shutdown(runtime);
    return 0;
}
