#include "device/hal/device.manager.h"
#include "acquire.h"
#include "platform.h"
#include "logger.h"

#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

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

static const size_t nframes = 70;
static const size_t image_width = 64;
static const size_t image_height = 48;

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
                                  sample_spacing_um,
                                  64 << 20));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = image_width,
                                             .y = image_height };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 1e4;
    props.video[0].max_frame_count = nframes;

    OK(acquire_configure(runtime, &props));
    OK(acquire_start(runtime));
    OK(acquire_stop(runtime));
}

int
main()
{
    auto runtime = acquire_init(reporter);
    acquire(runtime, TEST ".zarr");

    CHECK(fs::is_directory(TEST ".zarr"));
    const auto data_file_path = fs::path(TEST ".zarr/0/0/0/0/0");
    CHECK(fs::is_regular_file(data_file_path));

    const auto raw_file_size = nframes * image_width * image_height;
    CHECK(raw_file_size > fs::file_size(data_file_path));

    const auto zarray_path = fs::path(TEST ".zarr") / "0" / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    CHECK(fs::file_size(zarray_path) > 0);

    const auto external_metadata_path =
      fs::path(TEST ".zarr") / "0" / ".zattrs";
    CHECK(fs::is_regular_file(external_metadata_path));
    CHECK(fs::file_size(external_metadata_path) > 0);

    const auto group_zattrs_path = fs::path(TEST ".zarr") / ".zattrs";
    CHECK(fs::is_regular_file(group_zattrs_path));
    CHECK(fs::file_size(group_zattrs_path) > 0);

    LOG("Done (OK)");
    acquire_shutdown(runtime);
    return 0;
}
