/// @file write-zarr-v2-to-s3.cpp
/// @brief Test using Zarr V2 storage with S3 backend.

#if __has_include("credentials.hpp")

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <stdexcept>
#include <vector>

/// Defines the following constants:
/// - ZARR_S3_ENDPOINT ("http://...") - the URI of the S3 server
/// - ZARR_S3_BUCKET_NAME ("acquire-s3-test") - the name of the bucket
/// - ZARR_S3_ACCESS_KEY_ID - the access key ID for the S3 server
/// - ZARR_S3_SECRET_ACCESS_KEY - the secret access key for the S3 server
#include "credentials.hpp"

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

/// Check that strings a == b
/// example: `ASSERT_STREQ("foo",container_of_foo)`
#define ASSERT_STREQ(a, b)                                                     \
    do {                                                                       \
        std::string a_ = (a);                                                  \
        std::string b_ = (b);                                                  \
        EXPECT(a_ == b_,                                                       \
               "Expected %s==%s but '%s' != '%s'",                             \
               #a,                                                             \
               #b,                                                             \
               a_.c_str(),                                                     \
               b_.c_str());                                                    \
    } while (0)

/// Check that a>b
/// example: `ASSERT_GT(int,"%d",42,meaning_of_life())`
#define ASSERT_GT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ > b_, "Expected (%s) > (%s) but " fmt "<=" fmt, #a, #b, a_, b_);  \
    } while (0)

void
configure(AcquireRuntime* runtime)
{
    CHECK(runtime);

    const DeviceManager* dm = acquire_device_manager(runtime);
    CHECK(dm);

    AcquireProperties props = {};
    OK(acquire_get_configuration(runtime, &props));

    DEVOK(device_manager_select(dm,
                                DeviceKind_Camera,
                                SIZED("simulated.*empty.*"),
                                &props.video[0].camera.identifier));
    props.video[0].camera.settings.binning = 1;
    props.video[0].camera.settings.pixel_type = SampleType_u16;
    props.video[0].camera.settings.shape = { .x = 1920, .y = 1080 };
    // we may drop frames with lower exposure
    props.video[0].camera.settings.exposure_time_us = 2e5;

    props.video[0].max_frame_count = 100;

    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("ZarrBlosc1Lz4ByteShuffle"),
                                &props.video[0].storage.identifier));

    std::string uri = ZARR_S3_ENDPOINT "/" ZARR_S3_BUCKET_NAME "-v2";
    storage_properties_init(&props.video[0].storage.settings,
                            0,
                            uri.c_str(),
                            uri.length() + 1,
                            nullptr,
                            0,
                            {},
                            3);
    CHECK(storage_properties_set_access_key_and_secret(
      &props.video[0].storage.settings,
      SIZED(ZARR_S3_ACCESS_KEY_ID) + 1,
      SIZED(ZARR_S3_SECRET_ACCESS_KEY) + 1));

    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           0,
                                           SIZED("x") + 1,
                                           DimensionType_Space,
                                           1920,
                                           1920,
                                           1));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           1,
                                           SIZED("y") + 1,
                                           DimensionType_Space,
                                           1080,
                                           540,
                                           2));
    CHECK(storage_properties_set_dimension(&props.video[0].storage.settings,
                                           2,
                                           SIZED("t") + 1,
                                           DimensionType_Time,
                                           0,
                                           5,
                                           1));

    OK(acquire_configure(runtime, &props));
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
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration config;
    config.endpointOverride = ZARR_S3_ENDPOINT;
    const Aws::Auth::AWSCredentials credentials(ZARR_S3_ACCESS_KEY_ID,
                                                ZARR_S3_SECRET_ACCESS_KEY);
    auto client =
      std::make_shared<Aws::S3::S3Client>(credentials, nullptr, config);

    try {
        CHECK(client);

        std::vector<std::string> paths;
        paths.push_back(".metadata");
        paths.push_back(".zattrs");
        paths.push_back("0/.zarray");
        paths.push_back("0/.zattrs");

        for (auto i = 0; i < 20; ++i) {
            paths.push_back("0/" + std::to_string(i) + "/0/0");
            paths.push_back("0/" + std::to_string(i) + "/1/0");
        }

        for (const auto& path : paths) {
            Aws::S3::Model::HeadObjectRequest request;
            request.SetBucket(ZARR_S3_BUCKET_NAME "-v2");
            request.SetKey(path.c_str());
            auto outcome = client->HeadObject(request);
            CHECK(outcome.IsSuccess());
        }
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());

        Aws::ShutdownAPI(options);
        throw;
    } catch (...) {
        ERR("Unknown exception");

        Aws::ShutdownAPI(options);
        throw;
    }

    Aws::ShutdownAPI(options);
}

int
main()
{
    int retval = 1;
    AcquireRuntime* runtime = acquire_init(reporter);

    try {
        configure(runtime);
        acquire(runtime);
        validate(runtime);
        retval = 0;
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    acquire_shutdown(runtime);
    return retval;
}
#else
int
main()
{
    return 0;
}
#endif