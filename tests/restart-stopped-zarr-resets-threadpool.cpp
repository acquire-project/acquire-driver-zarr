/// @file tests/reuse-zarr-writer-resets-thread-pool
/// @brief Test that restarting a previously stopped Zarr writer resets the
/// thread pool.

#include "platform.h" // lib
#include "logger.h"
#include "device/kit/driver.h"
#include "device/hal/driver.h"
#include "device/hal/storage.h"
#include "device/props/storage.h"

#include <cstdio>
#include <string>
#include <stdexcept>
#include <vector>

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

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

typedef struct Driver* (*init_func_t)(void (*reporter)(int is_error,
                                                       const char* file,
                                                       int line,
                                                       const char* function,
                                                       const char* msg));

struct Storage*
get_zarr(lib* lib)
{

    CHECK(lib_open_by_name(lib, "acquire-driver-zarr"));

    auto init = (init_func_t)lib_load(lib, "acquire_driver_init_v0");
    auto driver = init(reporter);
    CHECK(driver);

    struct Storage* zarr = nullptr;
    for (uint32_t i = 0; i < driver->device_count(driver); ++i) {
        DeviceIdentifier id;
        DEVOK(driver->describe(driver, &id, i));
        std::string dev_name{ id.name };

        if (id.kind == DeviceKind_Storage && dev_name == "Zarr") {
            struct Device* device = nullptr;

            DEVOK(driver_open_device(driver, i, &device));
            zarr = containerof(device, struct Storage, device);
            break;
        }
    }

    return zarr;
}

void
configure(struct Storage* zarr)
{
    struct StorageProperties props = { 0 };
    storage_properties_init(&props, 0, SIZED(TEST ".zarr"), nullptr, 0, { 0 });

    CHECK(DeviceState_Armed == zarr->set(zarr, &props));
}

void
acquire_one_frame(struct Storage* zarr)
{
    CHECK(DeviceState_Running == zarr->start(zarr));
    struct ImageShape shape = {
        .dims = {
          .channels = 1,
          .width = 64,
          .height = 48,
          .planes = 1,
        },
        .strides = {
          .channels = 1,
          .width = 1,
          .height = 64,
          .planes = 64 * 48
        },
        .type = SampleType_u8,
    };
    zarr->reserve_image_shape(zarr, &shape);

    auto* frame = (struct VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
    frame->bytes_of_frame = sizeof(*frame) + 64 * 48;
    
    frame->shape = shape;
    frame->frame_id = 0;
    frame->hardware_frame_id = 0;
    frame->timestamps = { 0, 0 };

    size_t nbytes{ frame->bytes_of_frame };
    zarr->append(zarr, frame, &nbytes);

    free(frame);

    CHECK(DeviceState_Armed == zarr->stop(zarr));
}

int
main()
{
    logger_set_reporter(reporter);
    lib lib{};

    try {
        struct Storage* zarr = get_zarr(&lib);
        CHECK(zarr);

        configure(zarr);

        acquire_one_frame(zarr);
        acquire_one_frame(zarr); // thread pool should reset here

        lib_close(&lib);
        return 0;
    } catch (std::exception& e) {
        ERR("%s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    lib_close(&lib);
    return 1;
}