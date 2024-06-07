#include "benchmark.storage.hh"

#include "logger.h"
#include "platform.h"
#include "device/kit/driver.h"
#include "device/hal/driver.h"
#include "device/props/storage.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

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

static size_t
align_up(size_t n, size_t align)
{
    return (n + align - 1) & ~(align - 1);
}

typedef struct Driver* (*init_func_t)(void (*reporter)(int is_error,
                                                       const char* file,
                                                       int line,
                                                       const char* function,
                                                       const char* msg));

#define CHECK(e)                                                               \
    if (!(e)) {                                                                \
        std::stringstream ss;                                                  \
        ss << "Expression failed on line " << __LINE__ << ": " << #e;          \
        throw std::runtime_error(ss.str());                                    \
    }

namespace fs = std::filesystem;

namespace {
bool
is_s3_uri(const std::string& uri)
{
    return uri.starts_with("s3://") || uri.starts_with("http://") ||
           uri.starts_with("https://");
}

bool
init_zarr_driver(struct lib* lib)
{
    if (!lib) {
        return false;
    }
    logger_set_reporter(reporter);
    return lib_open_by_name(lib, "acquire-driver-zarr");
}

Storage*
make_storage(struct lib* lib, const std::string& storage_name)
{
    CHECK(lib);

    auto init = (init_func_t)lib_load(lib, "acquire_driver_init_v0");
    auto* driver = init(reporter);
    CHECK(driver);

    struct Storage* storage = nullptr;

    const uint32_t device_count = driver->device_count(driver);
    for (uint32_t i = 0; i < device_count; ++i) {
        DeviceIdentifier id{};
        CHECK(driver->describe(driver, &id, i) == Device_Ok);
        std::string name{ id.name };

        if (id.kind == DeviceKind_Storage && name == storage_name) {
            struct Device* device = nullptr;
            struct StoragePropertyMetadata metadata = { 0 };

            CHECK(Device_Ok == driver_open_device(driver, i, &device));
            storage = containerof(device, struct Storage, device);
            break;
        }
    }

    return storage;
}

void
destroy_zarr_driver(struct lib* lib)
{
    if (lib) {
        lib_close(lib);
    }
}
} // namespace

void
benchmark_storage(const std::string& storage_name,
                  std::vector<StorageProperties>& props_vec)
{
    using std::chrono::duration;
    using std::chrono::duration_cast;
    using std::chrono::high_resolution_clock;
    using std::chrono::milliseconds;

    auto* lib = new struct lib();
    CHECK(init_zarr_driver(lib))

    std::cout << "Benchmarking storage: " << storage_name << std::endl;
    struct Storage* storage = make_storage(lib, storage_name);
    CHECK(storage)

    duration<double, std::milli> elapsed{};

    for (const auto& props : props_vec) {
        CHECK(props.acquisition_dimensions.size == 3);

        CHECK(storage->set(storage, &props))

        const size_t frame_width =
          props.acquisition_dimensions.data[0].array_size_px;
        const size_t chunk_width =
          props.acquisition_dimensions.data[0].chunk_size_px;
        const size_t shard_width =
          props.acquisition_dimensions.data[0].shard_size_chunks;

        const size_t frame_height =
          props.acquisition_dimensions.data[1].array_size_px;
        const size_t chunk_height =
          props.acquisition_dimensions.data[1].chunk_size_px;
        const size_t shard_height =
          props.acquisition_dimensions.data[1].shard_size_chunks;

        const size_t chunk_planes =
          props.acquisition_dimensions.data[2].chunk_size_px;
        const size_t shard_planes =
          props.acquisition_dimensions.data[2].shard_size_chunks;

        struct ImageShape shape = {
            .dims = {
                .channels = 1,
                .width = props.acquisition_dimensions.data[0].array_size_px,
                .height = props.acquisition_dimensions.data[1].array_size_px,
                .planes = 1,
            },
            .strides = {
              .channels = 1,
              .width = 1,
              .height = props.acquisition_dimensions.data[0].array_size_px,
              .planes = props.acquisition_dimensions.data[0].array_size_px * props.acquisition_dimensions.data[1].array_size_px,
            }
        };

        storage->reserve_image_shape(storage, &shape);

        size_t nbytes_frame = shape.strides.planes; // u8

        auto* frame = (struct VideoFrame*)malloc(sizeof(struct VideoFrame) +
                                                 align_up(nbytes_frame, 8));
        CHECK(frame);
        frame->shape = shape;

        CHECK(storage->start(storage))
        const auto start = high_resolution_clock::now();
        for (auto i = 0; i < 100; ++i) {
            frame->frame_id = i;
            CHECK(storage->append(storage, frame, &nbytes_frame))
        }
        const auto end = high_resolution_clock::now();
        CHECK(storage->stop(storage))
        elapsed = end - start;
        std::cout << "frame width: " << frame_width << "; "
                  << "chunk width: " << chunk_width << "; "
                  << "shard width: " << shard_width << "; "
                  << "frame height: " << frame_height << "; "
                  << "chunk height: " << chunk_height << "; "
                  << "shard height: " << shard_height << "; "
                  << "chunk planes: " << chunk_planes << "; "
                  << "shard planes: " << shard_planes << "; "
                  << "Elapsed time: "
                  << duration_cast<milliseconds>(elapsed).count() << " ms"
                  << std::endl;

        free(frame);

        const std::string uri{ props.uri.str, props.uri.nbytes };
//        if (!is_s3_uri(uri)) {
//            fs::remove_all(uri);
//        }
    }

    storage_close(storage);

    destroy_zarr_driver(lib);
}