#include "s3.sink.hh"
#include "logger.hh"

#include <cstdlib>
#include <memory>

namespace {
bool
get_credentials(std::string& endpoint,
                std::string& bucket_name,
                std::string& access_key_id,
                std::string& secret_access_key)
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_ERROR("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_ERROR("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    bucket_name = env;

    if (!(env = std::getenv("ZARR_S3_ACCESS_KEY_ID"))) {
        LOG_ERROR("ZARR_S3_ACCESS_KEY_ID not set.");
        return false;
    }
    access_key_id = env;

    if (!(env = std::getenv("ZARR_S3_SECRET_ACCESS_KEY"))) {
        LOG_ERROR("ZARR_S3_SECRET_ACCESS_KEY not set.");
        return false;
    }
    secret_access_key = env;

    return true;
}
} // namespace

int
main()
{
    std::string s3_endpoint, bucket_name, s3_access_key_id,
      s3_secret_access_key;

    if (!get_credentials(
          s3_endpoint, bucket_name, s3_access_key_id, s3_secret_access_key)) {
        LOG_WARNING("Failed to get credentials. Skipping test.");
        return 0;
    }

    int retval = 1;
    const std::string object_name = "test-object";

    try {
        auto pool = std::make_shared<zarr::S3ConnectionPool>(
          1, s3_endpoint, s3_access_key_id, s3_secret_access_key);

        auto conn = pool->get_connection();
        if (!conn->check_connection()) {
            LOG_ERROR("Failed to connect to S3.");
            return 1;
        }
        CHECK(conn->bucket_exists(bucket_name));
        CHECK(conn->delete_object(bucket_name, object_name));
        CHECK(!conn->object_exists(bucket_name, object_name));

        pool->return_connection(std::move(conn));

        {
            const uint8_t str[] = "Hello, Acquire!";
            auto sink =
              std::make_unique<zarr::S3Sink>(bucket_name, object_name, pool);
            CHECK(sink->write(0, str, sizeof(str) - 1));
        }

        conn = pool->get_connection();
        CHECK(conn->object_exists(bucket_name, object_name));
        pool->return_connection(std::move(conn));

        // Verify the object contents.
        {
            minio::s3::BaseUrl url(s3_endpoint);
            url.https = s3_endpoint.starts_with("https://");

            minio::creds::StaticProvider provider(s3_access_key_id,
                                                  s3_secret_access_key);

            minio::s3::Client client(url, &provider);
            minio::s3::GetObjectArgs args;
            args.bucket = bucket_name;
            args.object = object_name;

            std::string contents;
            args.datafunc =
              [&contents](minio::http::DataFunctionArgs args) -> bool {
                contents = args.datachunk;
                return true;
            };

            // Call get object.
            minio::s3::GetObjectResponse resp = client.GetObject(args);

            if (contents != "Hello, Acquire!") {
                LOG_ERROR("Expected 'Hello, Acquire!' but got '%s'",
                          contents.c_str());
                return 1;
            }
        }

        // cleanup
        conn = pool->get_connection();
        CHECK(conn->delete_object(bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: %s", e.what());
    }

    return retval;
}