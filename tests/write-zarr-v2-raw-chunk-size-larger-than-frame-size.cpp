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

namespace {
void
init_array(struct StorageDimension** data, size_t size)
{
    *data = new struct StorageDimension[size];
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

const static uint32_t frame_width = 62, chunk_width = 64;
const static uint32_t frame_height = 46, chunk_height = 48;
const static uint32_t frames_per_chunk = 32;

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
                                 frames_per_chunk,
                                 0));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = frame_width,
                                             .y = frame_height };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 1e4;
    props.video[0].max_frame_count = frames_per_chunk;

    OK(acquire_configure(runtime, &props));

    const auto next = [](VideoFrame* cur) -> VideoFrame* {
        return (VideoFrame*)(((uint8_t*)cur) + cur->bytes_of_frame);
    };

    const auto consumed_bytes = [](const VideoFrame* const cur,
                                   const VideoFrame* const end) -> size_t {
        return (uint8_t*)end - (uint8_t*)cur;
    };

    struct clock clock;
    static double time_limit_ms = 20000.0;
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
                CHECK(cur->shape.dims.width ==
                      props.video[0].camera.settings.shape.x);
                CHECK(cur->shape.dims.height ==
                      props.video[0].camera.settings.shape.y);
                ++nframes;
            }
            {
                uint32_t n = consumed_bytes(beg, end);
                OK(acquire_unmap_read(runtime, 0, n));
                if (n)
                    LOG("stream %d consumed bytes %d", 0, n);
            }
            clock_sleep_ms(&throttle, 100.0f);

            LOG("stream %d expected_frames_per_chunk %d time %f",
                0,
                nframes,
                clock_toc_ms(&clock));
        } while (DeviceState_Running == acquire_get_state(runtime) &&
                 nframes < props.video[0].max_frame_count);

        OK(acquire_map_read(runtime, 0, &beg, &end));
        for (cur = beg; cur < end; cur = next(cur)) {
            LOG("stream %d counting frame w id %d", 0, cur->frame_id);
            CHECK(cur->shape.dims.width ==
                  props.video[0].camera.settings.shape.x);
            CHECK(cur->shape.dims.height ==
                  props.video[0].camera.settings.shape.y);
            ++nframes;
        }
        {
            uint32_t n = consumed_bytes(beg, end);
            OK(acquire_unmap_read(runtime, 0, n));
            if (n)
                LOG("stream %d consumed bytes %d", 0, n);
        }

        CHECK(nframes == props.video[0].max_frame_count);
    }

    OK(acquire_stop(runtime));
    storage_properties_destroy(&props.video[0].storage.settings);
}

void
validate()
{
    CHECK(fs::is_directory(TEST ".zarr"));

    const auto external_metadata_path =
      fs::path(TEST ".zarr") / "0" / ".zattrs";
    CHECK(fs::is_regular_file(external_metadata_path));
    ASSERT_GT(int, "%d", fs::file_size(external_metadata_path), 0);

    const auto group_zattrs_path = fs::path(TEST ".zarr") / ".zattrs";
    CHECK(fs::is_regular_file(group_zattrs_path));
    ASSERT_GT(int, "%d", fs::file_size(group_zattrs_path), 0);

    const auto zarray_path = fs::path(TEST ".zarr") / "0" / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));
    ASSERT_GT(int, "%d", fs::file_size(zarray_path), 0);

    // check metadata
    std::ifstream f(zarray_path);
    json zarray = json::parse(f);

    auto shape = zarray["shape"];
    ASSERT_EQ(int, "%d", frames_per_chunk, shape[0]);
    ASSERT_EQ(int, "%d", 1, shape[1]);
    ASSERT_EQ(int, "%d", frame_height, shape[2]);
    ASSERT_EQ(int, "%d", frame_width, shape[3]);

    auto chunks = zarray["chunks"];
    ASSERT_EQ(int, "%d", frames_per_chunk, chunks[0]);
    ASSERT_EQ(int, "%d", 1, chunks[1]);
    ASSERT_EQ(int, "%d", chunk_height, chunks[2]);
    ASSERT_EQ(int, "%d", chunk_width, chunks[3]);

    // check chunked data
    auto chunk_size = chunks[0].get<int>() * chunks[1].get<int>() *
                      chunks[2].get<int>() * chunks[3].get<int>();

    const auto chunk_file_path = fs::path(TEST ".zarr/0/0/0/0/0");
    CHECK(fs::is_regular_file(chunk_file_path));
    ASSERT_EQ(int, "%d", chunk_size, fs::file_size(chunk_file_path));
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
