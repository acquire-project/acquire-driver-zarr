/// @file write-zarr-v2-to-s3.cpp
/// @brief Test using Zarr V2 storage with S3 backend.

#include "device/hal/device.manager.h"
#include "acquire.h"
#include "logger.h"

#include <miniocpp/client.h>

#include <cstdlib>
#include <stdexcept>
#include <vector>

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
std::string s3_endpoint;
std::string s3_bucket_name;
std::string s3_access_key_id;
std::string s3_secret_access_key;

bool
get_credentials()
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        ERR("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    s3_endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        ERR("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    s3_bucket_name = env;

    if (!(env = std::getenv("ZARR_S3_ACCESS_KEY_ID"))) {
        ERR("ZARR_S3_ACCESS_KEY_ID not set.");
        return false;
    }
    s3_access_key_id = env;

    if (!(env = std::getenv("ZARR_S3_SECRET_ACCESS_KEY"))) {
        ERR("ZARR_S3_SECRET_ACCESS_KEY not set.");
        return false;
    }
    s3_secret_access_key = env;

    return true;
}

bool
bucket_exists(minio::s3::Client& client)
{
    if (s3_bucket_name.empty()) {
        return false;
    }

    try {
        minio::s3::BucketExistsArgs args;
        args.bucket = s3_bucket_name;

        minio::s3::BucketExistsResponse response = client.BucketExists(args);
        CHECK(response);

        return response.exist;
    } catch (const std::exception& e) {
        ERR("Failed to check existence of bucket: %s", e.what());
    }

    return false;
}

bool
remove_items(minio::s3::Client& client,
             const std::vector<std::string>& item_keys)
{
    std::list<minio::s3::DeleteObject> objects;
    for (const auto& key : item_keys) {
        minio::s3::DeleteObject object;
        object.name = key;
        objects.push_back(object);
    }

    try {
        minio::s3::RemoveObjectsArgs args;
        args.bucket = s3_bucket_name;

        auto it = objects.begin();

        args.func = [&objects = objects,
                     &i = it](minio::s3::DeleteObject& obj) -> bool {
            if (i == objects.end())
                return false;
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
        ERR("Failed to clear bucket %s: %s", s3_bucket_name.c_str(), e.what());
    }

    return false;
}

bool
object_exists(minio::s3::Client& client, const std::string& object_name)
{
    if (s3_bucket_name.empty() || object_name.empty()) {
        return false;
    }

    try {
        minio::s3::StatObjectArgs args;
        args.bucket = s3_bucket_name;
        args.object = object_name;

        minio::s3::StatObjectResponse response = client.StatObject(args);

        return (bool)response;
    } catch (const std::exception& e) {
        ERR("Failed to check if object %s exists in bucket %s: %s",
            object_name.c_str(),
            s3_bucket_name.c_str(),
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

    // check if the bucket already exists
    {
        minio::s3::BaseUrl url(s3_endpoint);
        url.https = s3_endpoint.starts_with("https://");

        minio::creds::StaticProvider provider(s3_access_key_id,
                                              s3_secret_access_key);

        minio::s3::Client client(url, &provider);

        CHECK(bucket_exists(client));
    }

    std::string uri = s3_endpoint + ("/" + s3_bucket_name);
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
      s3_access_key_id.c_str(),
      s3_access_key_id.size() + 1,
      s3_secret_access_key.c_str(),
      s3_secret_access_key.size() + 1
      ));

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
validate_and_cleanup(AcquireRuntime* runtime)
{
    std::vector<std::string> paths{
        ".zgroup",
        ".zattrs",
        "0/.zarray",
        "0/.zattrs",
    };
    for (auto i = 0; i < 20; ++i) {
        paths.push_back("0/" + std::to_string(i) + "/0/0");
        paths.push_back("0/" + std::to_string(i) + "/1/0");
    }

    minio::s3::BaseUrl url(s3_endpoint);
    url.https = s3_endpoint.starts_with("https://");

    minio::creds::StaticProvider provider(s3_access_key_id,
                                          s3_secret_access_key);

    minio::s3::Client client(url, &provider);
    CHECK(bucket_exists(client));

    try {
        for (const auto& path : paths) {
            CHECK(object_exists(client, path));
        }
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }
    remove_items(client, paths);

    CHECK(runtime);
    acquire_shutdown(runtime);
}

int
main()
{
    if (!get_credentials()) {
        return 0;
    }

    int retval = 1;
    AcquireRuntime* runtime = acquire_init(reporter);

    try {
        configure(runtime);
        acquire(runtime);
        validate_and_cleanup(runtime);
        retval = 0;
    } catch (const std::exception& e) {
        ERR("Exception: %s", e.what());
    } catch (...) {
        ERR("Unknown exception");
    }

    return retval;
}
