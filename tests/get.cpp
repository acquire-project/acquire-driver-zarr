/// @brief Check that Zarr devices implement the get_meta function.
/// Also check that both chunking and multiscale are marked as supported and
/// that the metadata for each is correct.

#include "platform.h"
#include "logger.h"
#include "device/kit/driver.h"
#include "device/hal/driver.h"
#include "device/hal/storage.h"
#include "device/props/storage.h"

#include <cstdio>
#include <cstring>
#include <string>

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

/// Helper for passing size static strings as function args.
/// For a function: `f(char*,size_t)` use `f(SIZED("hello"))`.
/// Expands to `f("hello",5)`.
#define SIZED(str) str, sizeof(str)

#define L aq_logger
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            goto Error;                                                        \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

/// Check that a==b
/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ == b_, "Expected %s==%s but " fmt "!=" fmt "\n", #a, #b, a_, b_); \
    } while (0)

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

int
main()
{
    logger_set_reporter(reporter);
    lib lib{};
    CHECK(lib_open_by_name(&lib, "acquire-driver-zarr"));
    {
        auto init = (init_func_t)lib_load(&lib, "acquire_driver_init_v0");
        auto driver = init(reporter);
        CHECK(driver);
        const auto n = driver->device_count(driver);
        for (uint32_t i = 0; i < n; ++i) {
            DeviceIdentifier id{};
            CHECK(driver->describe(driver, &id, i) == Device_Ok);

            std::string name{ id.name };

            if (id.kind == DeviceKind_Storage && name.starts_with("Zarr")) {
                struct Device* device = nullptr;
                struct Storage* storage = nullptr;

                CHECK(Device_Ok == driver_open_device(driver, i, &device));
                storage = containerof(device, struct Storage, device);

                struct StorageProperties props = { 0 };
                storage_properties_init(&props,
                                        13,
                                        SIZED(TEST ".zarr"),
                                        SIZED(R"({"foo":"bar"})"),
                                        { 1, 1 });
                props.chunk_dims_px = {
                    .width = 64,
                    .height = 48,
                    .planes = 6,
                };
                props.shard_dims_chunks = {
                    .width = 3,
                    .height = 2,
                    .planes = 1,
                };
                props.enable_multiscale = true;

                CHECK(Device_Ok == storage_set(storage, &props));
                props = { 0 };

                CHECK(Device_Ok == storage_get(storage, &props));

                CHECK(strcmp(props.filename.str, TEST ".zarr") == 0);
                CHECK(strcmp(props.external_metadata_json.str,
                             R"({"foo":"bar"})") == 0);

                CHECK(props.first_frame_id == 0); // this is ignored

                CHECK(props.chunk_dims_px.width == 64);
                CHECK(props.chunk_dims_px.height == 48);
                // 32 is the minimum value for planes
                CHECK(props.chunk_dims_px.planes == 32);

                if (name.starts_with("ZarrV3")) {
                    CHECK(props.shard_dims_chunks.width == 3);
                    CHECK(props.shard_dims_chunks.height == 2);
                    CHECK(props.shard_dims_chunks.planes == 1);
                } else {
                    CHECK(props.shard_dims_chunks.width == 0);
                    CHECK(props.shard_dims_chunks.height == 0);
                    CHECK(props.shard_dims_chunks.planes == 0);
                }

                CHECK(props.enable_multiscale == !name.starts_with("ZarrV3"));

                CHECK(Device_Ok == driver_close_device(device));
            }
        }
    }
    lib_close(&lib);
    return 0;
Error:
    lib_close(&lib);
    return 1;
}
