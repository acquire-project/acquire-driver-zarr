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
    ZarrDimensionSettings dimension;

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

    return true;

Error:
    return false;
}

bool
try_with_valid_settings()
{
    ZarrStreamSettings* settings;
    ZarrStream* stream;
    ZarrDimensionSettings dimension;
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

    CHECK_EQ(std::string(ZarrStream_get_store_path(stream)), store_path);
    CHECK_EQ(strlen(ZarrStream_get_s3_endpoint(stream)), 0);
    CHECK_EQ(strlen(ZarrStream_get_s3_bucket_name(stream)), 0);
    CHECK_EQ(strlen(ZarrStream_get_s3_access_key_id(stream)), 0);
    CHECK_EQ(strlen(ZarrStream_get_s3_secret_access_key(stream)), 0);

    CHECK_EQ(ZarrStream_get_compressor(stream), ZarrCompressor_None);
    CHECK_EQ(ZarrStream_get_compression_codec(stream),
             ZarrCompressionCodec_None);

    CHECK_EQ(ZarrStream_get_dimension_count(stream), 3);

    char name[64];
    ZarrDimensionType kind;
    size_t array_size_px, chunk_size_px, shard_size_chunks;
    CHECK_EQ(ZarrStream_get_dimension(stream,
                                      0,
                                      name,
                                      sizeof(name),
                                      &kind,
                                      &array_size_px,
                                      &chunk_size_px,
                                      &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "t");
    CHECK_EQ(kind, ZarrDimensionType_Time);
    CHECK_EQ(array_size_px, 1);
    CHECK_EQ(chunk_size_px, 1);
    CHECK_EQ(shard_size_chunks, 0);

    CHECK_EQ(ZarrStream_get_dimension(stream,
                                      1,
                                      name,
                                      sizeof(name),
                                      &kind,
                                      &array_size_px,
                                      &chunk_size_px,
                                      &shard_size_chunks),
             ZarrStatus_Success);

    CHECK_EQ(std::string(name), "y");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 12);
    CHECK_EQ(chunk_size_px, 3);
    CHECK_EQ(shard_size_chunks, 4);

    CHECK_EQ(ZarrStream_get_dimension(stream,
                                      2,
                                      name,
                                      sizeof(name),
                                      &kind,
                                      &array_size_px,
                                      &chunk_size_px,
                                      &shard_size_chunks),
             ZarrStatus_Success);

    CHECK_EQ(std::string(name), "x");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 10);
    CHECK_EQ(chunk_size_px, 5);
    CHECK_EQ(shard_size_chunks, 1);

    // check the store path was created
    CHECK(fs::is_directory(store_path));

    // check the store path contains the expected files
    CHECK(fs::is_regular_file(store_path + "/.zattrs"));
    CHECK(fs::is_regular_file(store_path + "/.zgroup"));
    CHECK(fs::is_directory(store_path + "/0"));
    CHECK(fs::is_regular_file(store_path + "/0/.zattrs"));

    ZarrStream_destroy(stream);

    // cleanup
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