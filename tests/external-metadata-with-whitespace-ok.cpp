/// @brief Check that setting external_metadata_json with trailing whitespace is
/// fine, actually. The old behavior was to check if the last character was '}',
/// but otherwise didn't validate JSON. This would fail if there was trailing
/// whitespace but otherwise had valid JSON. This test checks the new behavior,
/// which is to use nlohmann::json to parse the metadata. This has the added
/// benefit of actually validating the JSON.

#include "acquire.h"
#include "device/hal/device.manager.h"
#include "logger.h"

#include <cstdio>
#include <stdexcept>

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
#define SIZED(str) str, sizeof(str)

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
setup(AcquireRuntime* runtime)
{
    CHECK(runtime);
    auto dm = acquire_device_manager(runtime);
    CHECK(dm);

    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    DEVOK(device_manager_select(dm,
                                DeviceKind_Camera,
                                SIZED("simulated.*empty.*") - 1,
                                &props.video[0].camera.identifier));
    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("Zarr") - 1,
                                &props.video[0].storage.identifier));

    CHECK(storage_properties_init(
      &props.video[0].storage.settings,
      0,
      SIZED(TEST ".zarr"),
      SIZED(R"({"hello":"world"}  )"), // note trailing whitespace
      { 0 }));

    // we need at least 3 dimensions to validate settings
    auto* acq_dims = &props.video[0].storage.settings.acquisition_dimensions;
    acq_dims->init = init_array;
    acq_dims->destroy = destroy_array;
    CHECK(
      storage_properties_dimensions_init(&props.video[0].storage.settings, 3));

    CHECK(storage_dimension_init(
      acq_dims->data, SIZED("x") + 1, DimensionType_Space, 1, 1, 0));
    CHECK(storage_dimension_init(
      acq_dims->data + 1, SIZED("y") + 1, DimensionType_Space, 1, 1, 0));
    CHECK(storage_dimension_init(
      acq_dims->data + 2, SIZED("z") + 1, DimensionType_Space, 0, 1, 0));

    OK(acquire_configure(runtime, &props));

    storage_properties_destroy(&props.video[0].storage.settings);
}

int
main()
{
    int retval = 1;
    auto runtime = acquire_init(reporter);
    try {
        setup(runtime);

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