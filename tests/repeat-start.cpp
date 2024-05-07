/// @file tests/repeat-start
/// @brief Test that we can reuse a Zarr device after stopping it, with no error
/// in acquisition.

#include "logger.h"

#include "acquire.h"
#include "device/hal/device.manager.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "json.hpp"

namespace fs = std::filesystem;
using json = nlohmann::json;

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

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

/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
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

void
configure(AcquireRuntime* runtime)
{
    struct AcquireProperties props = { 0 };
    OK(acquire_get_configuration(runtime, &props));

    auto dm = acquire_device_manager(runtime);
    DEVOK(device_manager_select(dm,
                                DeviceKind_Camera,
                                SIZED("simulated.*random.*") - 1,
                                &props.video[0].camera.identifier));

    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u8;
    props.video[0].camera.settings.shape = { .x = 64, .y = 48 };
    props.video[0].camera.settings.exposure_time_us = 1e3;

    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("ZarrV3") - 1,
                                &props.video[0].storage.identifier));
    CHECK(storage_properties_init(&props.video[0].storage.settings,
                                  0,
                                  SIZED(TEST ".zarr"),
                                  nullptr,
                                  0,
                                  { 1, 1 },
                                  4));

    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           0,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           64,
                                           32,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           1,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           48,
                                           32,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           2,
                                           SIZED("c") + 1,
                                           DimensionType_Channel,
                                           1,
                                           1,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           3,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           32,
                                           1));

    props.video[0].max_frame_count = 10;

    OK(acquire_configure(runtime, &props));

    storage_properties_destroy(&props.video[0].storage.settings);
}

void
acquire(AcquireRuntime* runtime)
{
    acquire_start(runtime);
    acquire_stop(runtime);
}

void
validate(AcquireRuntime* runtime)
{
    AcquireProperties props = { 0 };
    OK(acquire_get_configuration(runtime, &props));

    std::string uri{ props.video[0].storage.settings.uri.str };
    uri.replace(uri.find("file://"), 7, "");
    const fs::path test_path(uri);
    EXPECT(fs::is_directory(test_path),
           "Expected %s to be a directory",
           test_path.string().c_str());

    // check the zarr.json metadata file
    fs::path metadata_path = test_path / "zarr.json";
    CHECK(fs::is_regular_file(metadata_path));
    std::ifstream f(metadata_path);
    json metadata = json::parse(f);

    CHECK(metadata["extensions"].empty());
    CHECK("https://purl.org/zarr/spec/protocol/core/3.0" ==
          metadata["metadata_encoding"]);
    CHECK(".json" == metadata["metadata_key_suffix"]);
    CHECK("https://purl.org/zarr/spec/protocol/core/3.0" ==
          metadata["zarr_format"]);

    // check the group metadata file
    metadata_path = test_path / "meta" / "root.group.json";
    CHECK(fs::is_regular_file(metadata_path));

    f = std::ifstream(metadata_path);
    metadata = json::parse(f);
    CHECK("" == metadata["attributes"]["acquire"]);

    // check the array metadata file
    metadata_path = test_path / "meta" / "root" / "0.array.json";
    CHECK(fs::is_regular_file(metadata_path));

    f = std::ifstream(metadata_path);
    metadata = json::parse(f);

    const auto chunk_grid = metadata["chunk_grid"];
    CHECK("/" == chunk_grid["separator"]);
    CHECK("regular" == chunk_grid["type"]);

    const auto chunk_shape = chunk_grid["chunk_shape"];
    ASSERT_EQ(int, "%d", 32, chunk_shape[0]);
    ASSERT_EQ(int, "%d", 1, chunk_shape[1]);
    ASSERT_EQ(int, "%d", 32, chunk_shape[2]);
    ASSERT_EQ(int, "%d", 32, chunk_shape[3]);

    CHECK("C" == metadata["chunk_memory_layout"]);
    CHECK("u1" == metadata["data_type"]);
    CHECK(metadata["extensions"].empty());

    const auto array_shape = metadata["shape"];
    ASSERT_EQ(int, "%d", 10, array_shape[0]);
    ASSERT_EQ(int, "%d", 1, array_shape[1]);
    ASSERT_EQ(int, "%d", 48, array_shape[2]);
    ASSERT_EQ(int, "%d", 64, array_shape[3]);

    // sharding
    const auto storage_transformers = metadata["storage_transformers"];
    const auto configuration = storage_transformers[0]["configuration"];
    const auto& cps = configuration["chunks_per_shard"];
    ASSERT_EQ(int, "%d", 1, cps[0]);
    ASSERT_EQ(int, "%d", 1, cps[1]);
    ASSERT_EQ(int, "%d", 1, cps[2]);
    ASSERT_EQ(int, "%d", 1, cps[3]);
    const size_t chunks_per_shard = cps[0].get<size_t>() *
                                    cps[1].get<size_t>() *
                                    cps[2].get<size_t>() * cps[3].get<size_t>();

    const auto index_size = 2 * chunks_per_shard * sizeof(uint64_t);

    // check that the chunked data file is the expected size
    const uint32_t bytes_per_chunk =
      chunk_shape[0].get<uint32_t>() * chunk_shape[1].get<uint32_t>() *
      chunk_shape[2].get<uint32_t>() * chunk_shape[3].get<uint32_t>();

    fs::path path = test_path / "data" / "root" / "0" / "c0" / "0" / "0" / "0";

    CHECK(fs::is_regular_file(path));

    auto file_size = fs::file_size(path);

    ASSERT_EQ(
      int, "%d", chunks_per_shard* bytes_per_chunk + index_size, file_size);
}

int
main()
{
    int retval = 1;
    auto runtime = acquire_init(reporter);

    try {
        configure(runtime);

        for (auto i = 0; i < 2; ++i) {
            acquire(runtime);
            validate(runtime);
        }

        retval = 0;
        LOG("Done (OK)");
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    acquire_shutdown(runtime);
    return retval;
}