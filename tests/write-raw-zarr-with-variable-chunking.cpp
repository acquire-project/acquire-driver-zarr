#include "acquire.h"
#include "device/hal/device.manager.h"
#include "platform.h" // clock
#include "logger.h"

#include <cmath> // std::ceil
#include <filesystem>
#include <string>
#include <stdexcept>
#include <vector>

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

/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
    } while (0)

namespace {
const size_t max_frames = 100;

struct Params
{
    uint32_t frame_x;
    uint32_t frame_y;
    size_t frames_per_chunk;

    [[nodiscard]] constexpr size_t bytes_per_frame() const
    {
        return frame_x * frame_y;
    }
    [[nodiscard]] constexpr size_t bytes_per_chunk() const
    {
        return bytes_per_frame() * frames_per_chunk;
    }
};
}

void
acquire(AcquireRuntime* runtime, const char* filename, const Params& params)
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
                                  params.bytes_per_chunk()));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = {
        .x = params.frame_x,
        .y = params.frame_y,
    };
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

    std::vector<Params> p;
    p.push_back({ .frame_x = 64, .frame_y = 48, .frames_per_chunk = 25 });
    p.push_back({ .frame_x = 96, .frame_y = 72, .frames_per_chunk = 66 });
    p.push_back({ .frame_x = 1920, .frame_y = 1080, .frames_per_chunk = 32 });

    for (const auto& params : p) {
        size_t bytes_per_chunk = params.bytes_per_chunk();

        acquire(runtime, TEST ".zarr", params);

        CHECK(fs::is_directory(TEST ".zarr"));

        size_t nchunks_expected =
          std::ceil((float)max_frames / (float)params.frames_per_chunk);
        for (int i = 0; i < nchunks_expected; ++i) {
            char filename[256];
            const size_t nbytes_s = strlen(TEST) + strlen(".zarr/0/0/0/0/") + 1;
            ASSERT_EQ(
              int,
              "%d",
              nbytes_s,
              snprintf(
                (char*)filename, nbytes_s + 1, "%s.zarr/0/%d/0/0/0", TEST, i));
            const auto chunk_path = fs::path(filename);
            CHECK(fs::is_regular_file(chunk_path));
            ASSERT_EQ(int, "%d", bytes_per_chunk, fs::file_size(chunk_path));
        }

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
    }

    OK(acquire_shutdown(runtime));
}
