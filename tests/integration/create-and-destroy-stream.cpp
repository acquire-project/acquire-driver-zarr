#include "zarr.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

// to be used in functions returning bool
#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "Assertion failed: %s\n", #cond);                  \
            goto Error;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

#define SIZED(name) name, sizeof(name)

namespace fs = std::filesystem;

bool
try_with_invalid_settings()
{
    ZarrStreamSettings* settings;
    ZarrStream* stream;
    ZarrDimensionProperties dimension;

    settings = ZarrStreamSettings_create();
    CHECK(settings);

    // reserve 3 dimensions, but only set 2 of them
    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 3),
             ZarrStatus_Success);

    dimension = {
        .name = "y",
        .bytes_of_name = strlen("y") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 12,
        .chunk_size_px = 3,
        .shard_size_chunks = 4,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 1, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "x",
        .bytes_of_name = strlen("x") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 10,
        .chunk_size_px = 5,
        .shard_size_chunks = 1,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 2, &dimension),
             ZarrStatus_Success);

    stream = ZarrStream_create(settings, ZarrVersion_2);
    CHECK(!stream);

    ZarrStreamSettings_destroy(settings);

    return true;

Error:
    return false;
}

bool
try_with_valid_settings()
{
    ZarrStreamSettings *settings, *stream_settings;
    ZarrStream* stream;
    ZarrS3Settings s3_settings;
    ZarrCompressionSettings compression_settings;
    ZarrDimensionProperties dimension;
    const std::string store_path = TEST ".zarr";

    settings = ZarrStreamSettings_create();
    CHECK(settings);

    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 3),
             ZarrStatus_Success);

    dimension = {
        .name = "t",
        .bytes_of_name = strlen("t") + 1,
        .kind = ZarrDimensionType_Time,
        .array_size_px = 1,
        .chunk_size_px = 1,
        .shard_size_chunks = 0,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 0, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "y",
        .bytes_of_name = strlen("y") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 12,
        .chunk_size_px = 3,
        .shard_size_chunks = 4,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 1, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "x",
        .bytes_of_name = strlen("x") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 10,
        .chunk_size_px = 5,
        .shard_size_chunks = 1,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 2, &dimension),
             ZarrStatus_Success);

    CHECK_EQ(ZarrStreamSettings_set_store(
               settings, store_path.c_str(), store_path.size() + 1, nullptr),
             ZarrStatus_Success);

    stream = ZarrStream_create(settings, ZarrVersion_2);
    CHECK(stream);

    // check the stream's settings are correct
    CHECK_EQ(ZarrStream_get_version(stream), ZarrVersion_2);

    stream_settings = ZarrStream_get_settings(stream);

    CHECK_EQ(std::string(ZarrStreamSettings_get_store_path(stream_settings)),
             store_path);

    s3_settings = ZarrStreamSettings_get_s3_settings(stream_settings);
    CHECK_EQ(strlen(s3_settings.endpoint), 0);
    CHECK_EQ(s3_settings.bytes_of_endpoint, 0);

    CHECK_EQ(strlen(s3_settings.bucket_name), 0);
    CHECK_EQ(s3_settings.bytes_of_bucket_name, 0);

    CHECK_EQ(strlen(s3_settings.access_key_id), 0);
    CHECK_EQ(s3_settings.bytes_of_access_key_id, 0);

    CHECK_EQ(strlen(s3_settings.secret_access_key), 0);
    CHECK_EQ(s3_settings.bytes_of_secret_access_key, 0);

    compression_settings = ZarrStreamSettings_get_compression(stream_settings);

    CHECK_EQ(compression_settings.compressor, ZarrCompressor_None);
    CHECK_EQ(compression_settings.codec, ZarrCompressionCodec_None);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(stream_settings), 3);

    dimension = ZarrStreamSettings_get_dimension(stream_settings, 0);
    CHECK_EQ(std::string(dimension.name), "t");
    CHECK_EQ(dimension.kind, ZarrDimensionType_Time);
    CHECK_EQ(dimension.array_size_px, 1);
    CHECK_EQ(dimension.chunk_size_px, 1);
    CHECK_EQ(dimension.shard_size_chunks, 0);

    dimension = ZarrStreamSettings_get_dimension(stream_settings, 1);
    CHECK_EQ(std::string(dimension.name), "y");
    CHECK_EQ(dimension.kind, ZarrDimensionType_Space);
    CHECK_EQ(dimension.array_size_px, 12);
    CHECK_EQ(dimension.chunk_size_px, 3);
    CHECK_EQ(dimension.shard_size_chunks, 4);

    dimension = ZarrStreamSettings_get_dimension(stream_settings, 2);
    CHECK_EQ(std::string(dimension.name), "x");
    CHECK_EQ(dimension.kind, ZarrDimensionType_Space);
    CHECK_EQ(dimension.array_size_px, 10);
    CHECK_EQ(dimension.chunk_size_px, 5);
    CHECK_EQ(dimension.shard_size_chunks, 1);

    // check the store path was created
    CHECK(fs::is_directory(store_path));

    // check the store path contains the expected files
    CHECK(fs::is_regular_file(store_path + "/.zattrs"));
    CHECK(fs::is_regular_file(store_path + "/.zgroup"));
    CHECK(fs::is_directory(store_path + "/0"));
    CHECK(fs::is_regular_file(store_path + "/0/.zattrs"));

    ZarrStream_destroy(stream);

    // cleanup
    ZarrStreamSettings_destroy(settings);
    ZarrStreamSettings_destroy(stream_settings);
    try {
        fs::remove_all(store_path);
    } catch (const fs::filesystem_error& e) {
        fprintf(
          stderr, "Failed to remove %s: %s\n", store_path.c_str(), e.what());
        return false;
    }

    return true;

Error:
    return false;
}

int
main()
{
    int retval = 0;

    CHECK(try_with_invalid_settings());
    CHECK(try_with_valid_settings());

Finalize:
    return retval;

Error:
    retval = 1;
    goto Finalize;
}