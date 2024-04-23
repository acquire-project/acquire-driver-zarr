/// @file get.cpp
/// Check that Zarr devices correctly implement the `get` Storage API function.

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

                // unconfigured behavior
                CHECK(storage_get(storage, &props) == Device_Ok);

                CHECK(props.uri.str);
                CHECK(strcmp(props.uri.str, "") == 0);
                CHECK(props.uri.nbytes == 1);

                CHECK(props.external_metadata_json.str);
                CHECK(strcmp(props.external_metadata_json.str, "") == 0);
                CHECK(props.external_metadata_json.nbytes == 1);

                CHECK(props.first_frame_id == 0);

                CHECK(props.pixel_scale_um.x == 1);
                CHECK(props.pixel_scale_um.y == 1);

                CHECK(props.acquisition_dimensions.size == 0);
                CHECK(props.acquisition_dimensions.data == nullptr);

                CHECK(props.enable_multiscale == 0);

                CHECK(storage_properties_init(
                  &props,
                  13,
                  SIZED(TEST ".zarr"),
                  SIZED(R"({"foo":"bar"})"),
                  { 1, 1 },
                  3 // we need at least 3 dimensions to validate settings
                  ));

                CHECK(storage_properties_set_dimension(
                  &props, 0, SIZED("x") + 1, DimensionType_Space, 64, 16, 2));
                CHECK(storage_properties_set_dimension(
                  &props, 1, SIZED("y") + 1, DimensionType_Space, 48, 16, 3));
                CHECK(storage_properties_set_dimension(
                  &props, 2, SIZED("z") + 1, DimensionType_Space, 0, 6, 1));

                props.enable_multiscale = true;

                // configure the storage device
                CHECK(Device_Ok == storage_set(storage, &props));
                CHECK(Device_Ok == storage_get(storage, &props));

                CHECK(strcmp(props.uri.str, TEST ".zarr") == 0);
                CHECK(strcmp(props.external_metadata_json.str,
                             R"({"foo":"bar"})") == 0);

                CHECK(props.acquisition_dimensions.size == 3);
                CHECK(props.acquisition_dimensions.data != nullptr);

                CHECK(0 == strcmp(props.acquisition_dimensions.data[0].name.str,
                                  "x"));
                CHECK(DimensionType_Space ==
                      props.acquisition_dimensions.data[0].kind);
                CHECK(props.acquisition_dimensions.data[0].array_size_px == 64);
                CHECK(props.acquisition_dimensions.data[0].chunk_size_px == 16);
                CHECK(props.acquisition_dimensions.data[0].shard_size_chunks ==
                      2);

                CHECK(0 == strcmp(props.acquisition_dimensions.data[1].name.str,
                                  "y"));
                CHECK(DimensionType_Space ==
                      props.acquisition_dimensions.data[1].kind);
                CHECK(props.acquisition_dimensions.data[1].array_size_px == 48);
                CHECK(props.acquisition_dimensions.data[1].chunk_size_px == 16);
                CHECK(props.acquisition_dimensions.data[1].shard_size_chunks ==
                      3);

                CHECK(0 == strcmp(props.acquisition_dimensions.data[2].name.str,
                                  "z"));
                CHECK(DimensionType_Space ==
                      props.acquisition_dimensions.data[2].kind);
                CHECK(props.acquisition_dimensions.data[2].array_size_px == 0);
                CHECK(props.acquisition_dimensions.data[2].chunk_size_px == 6);
                CHECK(props.acquisition_dimensions.data[2].shard_size_chunks ==
                      1);

                CHECK(props.first_frame_id == 0); // this is ignored

                CHECK(props.enable_multiscale == !name.starts_with("ZarrV3"));

                storage_properties_destroy(&props);

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
