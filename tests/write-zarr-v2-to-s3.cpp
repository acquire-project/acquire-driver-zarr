/// @file write-zarr-v2-to-s3.cpp
/// @brief Test using Zarr V2 storage with S3 backend.

#if __has_include("s3.credentials.hh")

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

#include <miniocpp/client.h>

#include <stdexcept>
#include <vector>

/// Defines the following constants:
/// - ZARR_S3_ENDPOINT ("http://...") - the URI of the S3 server
/// - ZARR_S3_ACCESS_KEY_ID - the access key ID for the S3 server
/// - ZARR_S3_SECRET_ACCESS_KEY - the secret access key for the S3 server
#include "s3.credentials.hh"

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

namespace {
const std::string bucket_name = "write-zarr-v2-to-s3";

bool
bucket_exists(minio::s3::Client& client)
{
    if (bucket_name.empty()) {
        return false;
    }

    try {
        minio::s3::BucketExistsArgs args;
        args.bucket = bucket_name;

        minio::s3::BucketExistsResponse response = client.BucketExists(args);
        CHECK(response);

        return response.exist;
    } catch (const std::exception& e) {
        ERR("Failed to check existence of bucket: %s", e.what());
    }

    return false;
}

bool
clear_bucket(minio::s3::Client& client)
{
    if (bucket_name.empty()) {
        return false;
    }

    if (!bucket_exists(client)) {
        return true;
    }

    LOG("Clearing bucket %s", bucket_name.c_str());

    std::list<minio::s3::DeleteObject> objects;

    try {
        minio::s3::ListObjectsArgs args;
        args.bucket = bucket_name;
        args.recursive = true;
        minio::s3::ListObjectsResult result = client.ListObjects(args);

        for (; result; result++) {
            minio::s3::Item item = *result;
            minio::s3::DeleteObject object;
            object.name = item.name;
            objects.push_back(object);
        }
    } catch (const std::exception& e) {
        ERR("Failed to clear bucket %s: %s", bucket_name.c_str(), e.what());
        return false;
    }

    try {
        minio::s3::RemoveObjectsArgs args;
        args.bucket = bucket_name;

        std::list<minio::s3::DeleteObject>::iterator it = objects.begin();

        args.func = [&objects = objects,
                     &i = it](minio::s3::DeleteObject& obj) -> bool {
            if (i == objects.end()) return false;
            obj = *i;
            i++;
            return true;
        };

        minio::s3::RemoveObjectsResult result = client.RemoveObjects(args);
        for (; result; result++) {
            minio::s3::DeleteError err = *result;
            if (!err) {
                ERR("Failed to delete object %s: %s",
                    err.object_name.c_str(),
                    err.message.c_str());
                return false;
            }
        }

        return true;
    } catch (const std::exception& e) {
        ERR("Failed to clear bucket %s: %s", bucket_name.c_str(), e.what());
    }

    return false;
}

bool
destroy_bucket(minio::s3::Client& client)
{
    if (bucket_name.empty()) {
        return false;
    }

    // first check if the bucket exists, do nothing if it does
    if (!bucket_exists(client)) {
        return true;
    }

    // clear the bucket before destroying it
    CHECK(clear_bucket(client));

    LOG("Destroying bucket %s", bucket_name.c_str());
    minio::s3::RemoveBucketArgs args;
    args.bucket = bucket_name;

    try {
        minio::s3::RemoveBucketResponse response = client.RemoveBucket(args);

        return (bool)response;
    } catch (const std::exception& e) {
        ERR("Failed to destroy bucket %s: %s", bucket_name.c_str(), e.what());
    }

    return false;
}

bool
object_exists(minio::s3::Client& client, const std::string& object_name)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    try {
        minio::s3::StatObjectArgs args;
        args.bucket = bucket_name;
        args.object = object_name;

        minio::s3::StatObjectResponse response = client.StatObject(args);

        return (bool)response;
    } catch (const std::exception& e) {
        ERR("Failed to check if object %s exists in bucket %s: %s",
            object_name.c_str(),
            bucket_name.c_str(),
            e.what());
    }

    return false;
}
} // namespace

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
    props.video[0].camera.settings.exposure_time_us = 1e4;

    props.video[0].max_frame_count = 100;

    DEVOK(device_manager_select(dm,
                                DeviceKind_Storage,
                                SIZED("ZarrBlosc1Lz4ByteShuffle"),
                                &props.video[0].storage.identifier));

    // destroy the bucket if it already exists
    {
        const std::string endpoint = ZARR_S3_ENDPOINT;
        minio::s3::BaseUrl url(endpoint);
        url.https = endpoint.starts_with("https://");

        minio::creds::StaticProvider provider(ZARR_S3_ACCESS_KEY_ID,
                                              ZARR_S3_SECRET_ACCESS_KEY);

        minio::s3::Client client(url, &provider);

        CHECK(destroy_bucket(client));
    }

    std::string uri = ZARR_S3_ENDPOINT + ("/" + bucket_name);
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
    std::string endpoint = ZARR_S3_ENDPOINT;
    minio::s3::BaseUrl url(endpoint);
    url.https = endpoint.starts_with("https://");

    minio::creds::StaticProvider provider(ZARR_S3_ACCESS_KEY_ID,
                                          ZARR_S3_SECRET_ACCESS_KEY);

    minio::s3::Client client(url, &provider);

    CHECK(bucket_exists(client));

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
        CHECK(object_exists(client, path));
    }
}

void
cleanup(AcquireRuntime* runtime) noexcept
{
    if (runtime) {
        acquire_shutdown(runtime);
    }

    try {
        const std::string endpoint = ZARR_S3_ENDPOINT;
        minio::s3::BaseUrl url(endpoint);
        url.https = endpoint.starts_with("https://");

        minio::creds::StaticProvider provider(ZARR_S3_ACCESS_KEY_ID,
                                              ZARR_S3_SECRET_ACCESS_KEY);

        minio::s3::Client client(url, &provider);

        CHECK(destroy_bucket(client));
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }
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

    cleanup(runtime);
    return retval;
}
#else
int
main()
{
    return 0;
}
#endif