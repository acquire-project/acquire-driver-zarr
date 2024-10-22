#include "platform.h"
#include "logger.h"
#include "device/kit/storage.h"
#include "device/kit/driver.h"
#include "device/hal/storage.h"
#include "device/hal/driver.h"

#include <chrono>
#include <cstdio>
#include <iostream>
#include <filesystem>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

#define L aq_logger
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Expression evaluated as false");         \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

#define SIZED(a) a, sizeof(a)

namespace {
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

bool lib_loaded = false;
auto* lib = new struct lib
{};
struct Driver* driver = nullptr;
struct Device* device = nullptr;

typedef struct Driver* (*init_func_t)(void (*reporter)(int is_error,
                                                       const char* file,
                                                       int line,
                                                       const char* function,
                                                       const char* msg));

void
open_device(std::string_view device_name)
{
    if (!lib_loaded) {
        CHECK(lib_open_by_name(lib, "acquire-driver-zarr"));
        lib_loaded = true;
    }

    if (!driver) {
        auto init = (init_func_t)lib_load(lib, "acquire_driver_init_v0");
        driver = init(reporter);
    }

    if (device) {
        driver_close_device(device);
        device = nullptr;
    }

    CHECK(driver);
    const auto n = driver->device_count(driver);
    for (auto i = 0; i < n; ++i) {
        DeviceIdentifier id{};
        CHECK(driver->describe(driver, &id, i) == Device_Ok);
        if (id.kind != DeviceKind_Storage) {
            continue;
        }

        if (strcmp(device_name.data(), id.name) == 0) {
            CHECK(Device_Ok ==
                  driver_open_device(driver, id.device_id, &device));
            break;
        }
    }
}

struct Storage*
get_zarr_v3()
{
    struct Storage* storage = nullptr;

    if (!device) {
        open_device("ZarrV3");
    }
    storage = containerof(device, struct Storage, device);
    return storage;
}

void
cleanup()
{
    if (device) {
        driver_close_device(device);
        device = nullptr;
    }

    if (driver) {
        driver->shutdown(driver);
        driver = nullptr;
    }

    if (lib) {
        lib_close(lib);
        delete lib;
        lib = nullptr;
    }

    const auto file_path = (fs::temp_directory_path() / "test.zarr").string();
    if (fs::exists(file_path)) {
        fs::remove_all(file_path);
    }
}
} // namespace

void
configure(struct Storage* storage)
{
    StorageProperties props{};

    const auto file_path = (fs::temp_directory_path() / "test.zarr").string();
    storage_properties_init(
      &props, 0, file_path.c_str(), file_path.size() + 1, nullptr, 0, {}, 3);

    storage_properties_set_dimension(
      &props, 0, SIZED("x"), DimensionType_Space, 14192, 64, 222);
    storage_properties_set_dimension(
      &props, 1, SIZED("y"), DimensionType_Space, 10640, 64, 167);
    storage_properties_set_dimension(
      &props, 2, SIZED("t"), DimensionType_Time, 0, 64, 1);

    props.enable_multiscale = 0;

    CHECK(storage_set(storage, &props) == Device_Ok);

    storage_properties_destroy(&props);
}

void
print_usage(const char* program_name)
{
    std::cerr << "Usage: " << program_name << " <number_of_iterations>\n"
              << "Example: " << program_name << " 100\n";
}

int
main(int argc, char* argv[])
{
    if (argc != 2) {
        print_usage(argv[0]);
        return 1;
    }

    long iters;

    {
        char* end;
        iters = std::strtol(argv[1], &end, 10);

        // Validate the input
        if (*end != '\0' || iters <= 0) {
            std::cerr << "Error: Please provide a valid positive number of "
                         "iterations\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    struct Storage* storage = get_zarr_v3();
    CHECK(storage);

    configure(storage);

    size_t bytes_of_frame = sizeof(struct VideoFrame) + 14192 * 10640 * 2;
    auto* frame = (struct VideoFrame*)malloc(bytes_of_frame);
    memset(frame, 0, bytes_of_frame);

    frame->bytes_of_frame = bytes_of_frame;
    frame->shape = {
        .dims = { .channels = 1, .width = 14192, .height = 10640, .planes = 1, },
        .strides = { .channels = 1,
          .width = 1,
          .height = 14192,
          .planes = 14192 * 10640, },
        .type = SampleType_u16,
    };

    CHECK(storage_reserve_image_shape(storage, &frame->shape) == Device_Ok);

    CHECK(storage_start(storage) == Device_Ok);

    auto start = std::chrono::high_resolution_clock::now();
    for (auto i = 0; i < iters; ++i) {
        frame->frame_id = i;
        CHECK(storage_append(storage, frame, frame + 1) == Device_Ok);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Execution time of the loop: " << duration.count()
              << " milliseconds" << std::endl;

    CHECK(storage_stop(storage) == Device_Ok);

    cleanup();
    return 0;
}