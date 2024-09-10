#include "zarr.h"

#include <cstdio>
#include <string>

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "Assertion failed: %s\n", #cond);                  \
            goto Error;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

#define SIZED(name) name, sizeof(name)

bool
check_preconditions(ZarrStreamSettings* stream)
{
    std::string str_param_value;

    str_param_value = ZarrStreamSettings_get_store_path(stream);
    CHECK(str_param_value.empty());

    str_param_value = ZarrStreamSettings_get_s3_endpoint(stream);
    CHECK(str_param_value.empty());

    str_param_value = ZarrStreamSettings_get_s3_bucket_name(stream);
    CHECK(str_param_value.empty());

    str_param_value = ZarrStreamSettings_get_s3_access_key_id(stream);
    CHECK(str_param_value.empty());

    str_param_value = ZarrStreamSettings_get_s3_secret_access_key(stream);
    CHECK(str_param_value.empty());

    CHECK_EQ(ZarrStreamSettings_get_data_type(stream), ZarrDataType_uint8);

    CHECK_EQ(ZarrStreamSettings_get_compressor(stream), ZarrCompressor_None);

    CHECK_EQ(ZarrStreamSettings_get_compression_codec(stream),
             ZarrCompressionCodec_None);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(stream), 0);

    CHECK_EQ(ZarrStreamSettings_get_multiscale(stream), 0);

    return true;

Error:
    return false;
}

bool
set_and_get_parameters(ZarrStreamSettings* settings)
{
    std::string str_param_value;

    /* Set and get store */
    ZarrS3Settings s3_settings{
        .endpoint = "s3_endpoint",
        .bytes_of_endpoint = sizeof("s3_endpoint"),
        .bucket_name = "s3_bucket_name",
        .bytes_of_bucket_name = sizeof("s3_bucket_name"),
        .access_key_id = "s3_access_key_id",
        .bytes_of_access_key_id = sizeof("s3_access_key_id"),
        .secret_access_key = "s3_secret_access_key",
        .bytes_of_secret_access_key = sizeof("s3_secret_access_key"),
    };

    ZarrCompressionSettings compression_settings{
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 5,
        .shuffle = 1,
    };

    ZarrStatus status = ZarrStreamSettings_set_store(
      settings, "store_path", sizeof("store_path"), &s3_settings);
    CHECK_EQ(status, ZarrStatus_Success);

    str_param_value = ZarrStreamSettings_get_store_path(settings);
    CHECK_EQ(str_param_value, "store_path");

    str_param_value = ZarrStreamSettings_get_s3_endpoint(settings);
    CHECK_EQ(str_param_value, "s3_endpoint");

    str_param_value = ZarrStreamSettings_get_s3_bucket_name(settings);
    CHECK_EQ(str_param_value, "s3_bucket_name");

    str_param_value = ZarrStreamSettings_get_s3_access_key_id(settings);
    CHECK_EQ(str_param_value, "s3_access_key_id");

    str_param_value = ZarrStreamSettings_get_s3_secret_access_key(settings);
    CHECK_EQ(str_param_value, "s3_secret_access_key");

    /* Set and get data type */
    CHECK_EQ(ZarrStreamSettings_set_data_type(settings, ZarrDataType_float16),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_get_data_type(settings), ZarrDataType_float16);

    /* Set and get compression settings */
    CHECK_EQ(
      ZarrStreamSettings_set_compression(settings, &compression_settings),
      ZarrStatus_Success);

    /* Set and get some dimensions */
    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 5),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 4, SIZED("x"), ZarrDimensionType_Space, 10, 5, 2),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 3, SIZED("y"), ZarrDimensionType_Space, 20, 10, 3),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 2, SIZED("z"), ZarrDimensionType_Space, 30, 15, 4),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 1, SIZED("c"), ZarrDimensionType_Channel, 40, 20, 5),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 0, SIZED("t"), ZarrDimensionType_Time, 50, 25, 6),
             ZarrStatus_Success);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(settings), 5);

    char name[64];
    ZarrDimensionType kind;
    size_t array_size_px;
    size_t chunk_size_px;
    size_t shard_size_chunks;

    CHECK_EQ(ZarrStreamSettings_get_dimension(settings,
                                              0,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "t");
    CHECK_EQ(kind, ZarrDimensionType_Time);
    CHECK_EQ(array_size_px, 50);
    CHECK_EQ(chunk_size_px, 25);
    CHECK_EQ(shard_size_chunks, 6);

    CHECK_EQ(ZarrStreamSettings_get_dimension(settings,
                                              1,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "c");
    CHECK_EQ(kind, ZarrDimensionType_Channel);
    CHECK_EQ(array_size_px, 40);
    CHECK_EQ(chunk_size_px, 20);
    CHECK_EQ(shard_size_chunks, 5);

    CHECK_EQ(ZarrStreamSettings_get_dimension(settings,
                                              2,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "z");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 30);
    CHECK_EQ(chunk_size_px, 15);
    CHECK_EQ(shard_size_chunks, 4);

    CHECK_EQ(ZarrStreamSettings_get_dimension(settings,
                                              3,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "y");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 20);
    CHECK_EQ(chunk_size_px, 10);
    CHECK_EQ(shard_size_chunks, 3);

    CHECK_EQ(ZarrStreamSettings_get_dimension(settings,
                                              4,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrStatus_Success);
    CHECK_EQ(std::string(name), "x");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 10);
    CHECK_EQ(chunk_size_px, 5);
    CHECK_EQ(shard_size_chunks, 2);

    /* Set and get multiscale */
    CHECK_EQ(ZarrStreamSettings_set_multiscale(settings, 10),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_get_multiscale(settings), 1); // normalized to 1

    return true;

Error:
    return false;
}

int
main()
{
    ZarrStreamSettings* stream = ZarrStreamSettings_create();
    if (!stream) {
        fprintf(stderr, "Failed to create Zarr stream\n");
        return 1;
    }

    int retval = 0;

    CHECK(check_preconditions(stream));
    CHECK(set_and_get_parameters(stream));

Finalize:
    ZarrStreamSettings_destroy(stream);
    return retval;
Error:
    retval = 1;
    goto Finalize;
}