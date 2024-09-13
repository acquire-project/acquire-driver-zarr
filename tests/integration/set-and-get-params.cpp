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
check_preconditions(ZarrStreamSettings* settings)
{
    std::string str_param_value;
    ZarrS3Settings s3_settings;
    ZarrCompressionSettings compression_settings;

    str_param_value = ZarrStreamSettings_get_store_path(settings);
    CHECK(str_param_value.empty());

    s3_settings = ZarrStreamSettings_get_s3_settings(settings);
    str_param_value = s3_settings.endpoint;
    CHECK(str_param_value.empty());

    str_param_value = s3_settings.bucket_name;
    CHECK(str_param_value.empty());

    str_param_value = s3_settings.access_key_id;
    CHECK(str_param_value.empty());

    str_param_value = s3_settings.secret_access_key;
    CHECK(str_param_value.empty());

    CHECK_EQ(ZarrStreamSettings_get_data_type(settings), ZarrDataType_uint8);

    compression_settings = ZarrStreamSettings_get_compression(settings);
    CHECK_EQ(compression_settings.compressor, ZarrCompressor_None);

    CHECK_EQ(compression_settings.codec, ZarrCompressionCodec_None);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(settings), 0);

    CHECK_EQ(ZarrStreamSettings_get_multiscale(settings), 0);

    return true;

Error:
    return false;
}

bool
set_and_get_parameters(ZarrStreamSettings* settings)
{
    std::string str_param_value;

    /* Set and get store */
    ZarrS3Settings s3_settings_in{
        .endpoint = "s3_endpoint",
        .bytes_of_endpoint = sizeof("s3_endpoint"),
        .bucket_name = "s3_bucket_name",
        .bytes_of_bucket_name = sizeof("s3_bucket_name"),
        .access_key_id = "s3_access_key_id",
        .bytes_of_access_key_id = sizeof("s3_access_key_id"),
        .secret_access_key = "s3_secret_access_key",
        .bytes_of_secret_access_key = sizeof("s3_secret_access_key"),
    };
    ZarrS3Settings s3_settings_out;

    ZarrCompressionSettings compression_settings_in{
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 5,
        .shuffle = 1,
    };
    ZarrCompressionSettings compression_settings_out;

    ZarrDimensionProperties dimension;

    ZarrStatus status = ZarrStreamSettings_set_store(
      settings, "store_path", sizeof("store_path"), &s3_settings_in);
    CHECK_EQ(status, ZarrStatus_Success);

    str_param_value = ZarrStreamSettings_get_store_path(settings);
    CHECK_EQ(str_param_value, "store_path");

    s3_settings_out = ZarrStreamSettings_get_s3_settings(settings);
    str_param_value = s3_settings_out.endpoint;
    CHECK_EQ(str_param_value, "s3_endpoint");

    str_param_value = s3_settings_out.bucket_name;
    CHECK_EQ(str_param_value, "s3_bucket_name");

    str_param_value = s3_settings_out.access_key_id;
    CHECK_EQ(str_param_value, "s3_access_key_id");

    str_param_value = s3_settings_out.secret_access_key;
    CHECK_EQ(str_param_value, "s3_secret_access_key");

    /* Set and get data type */
    CHECK_EQ(ZarrStreamSettings_set_data_type(settings, ZarrDataType_float32),
             ZarrStatus_Success);
    CHECK_EQ(ZarrStreamSettings_get_data_type(settings), ZarrDataType_float32);

    /* Set and get compression settings */
    CHECK_EQ(
      ZarrStreamSettings_set_compression(settings, &compression_settings_in),
      ZarrStatus_Success);

    /* Set and get some dimensions */
    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 5),
             ZarrStatus_Success);

    dimension = {
        .name = "t",
        .bytes_of_name = sizeof("t"),
        .kind = ZarrDimensionType_Time,
        .array_size_px = 50,
        .chunk_size_px = 25,
        .shard_size_chunks = 6,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 0, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "c",
        .bytes_of_name = sizeof("c"),
        .kind = ZarrDimensionType_Channel,
        .array_size_px = 40,
        .chunk_size_px = 20,
        .shard_size_chunks = 5,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 1, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "z",
        .bytes_of_name = sizeof("z"),
        .kind = ZarrDimensionType_Space,
        .array_size_px = 30,
        .chunk_size_px = 15,
        .shard_size_chunks = 4,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 2, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "y",
        .bytes_of_name = sizeof("y"),
        .kind = ZarrDimensionType_Space,
        .array_size_px = 20,
        .chunk_size_px = 10,
        .shard_size_chunks = 3,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 3, &dimension),
             ZarrStatus_Success);

    dimension = {
        .name = "x",
        .bytes_of_name = sizeof("x"),
        .kind = ZarrDimensionType_Space,
        .array_size_px = 10,
        .chunk_size_px = 5,
        .shard_size_chunks = 2,
    };
    CHECK_EQ(ZarrStreamSettings_set_dimension(settings, 4, &dimension),
             ZarrStatus_Success);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(settings), 5);

    ZarrDimensionProperties dimension_out;

    dimension_out = ZarrStreamSettings_get_dimension(settings, 0);
    CHECK_EQ(std::string(dimension_out.name), "t");
    CHECK_EQ(dimension_out.kind, ZarrDimensionType_Time);
    CHECK_EQ(dimension_out.array_size_px, 50);
    CHECK_EQ(dimension_out.chunk_size_px, 25);
    CHECK_EQ(dimension_out.shard_size_chunks, 6);

    dimension_out = ZarrStreamSettings_get_dimension(settings, 1);
    CHECK_EQ(std::string(dimension_out.name), "c");
    CHECK_EQ(dimension_out.kind, ZarrDimensionType_Channel);
    CHECK_EQ(dimension_out.array_size_px, 40);
    CHECK_EQ(dimension_out.chunk_size_px, 20);
    CHECK_EQ(dimension_out.shard_size_chunks, 5);

    dimension_out = ZarrStreamSettings_get_dimension(settings, 2);
    CHECK_EQ(std::string(dimension_out.name), "z");
    CHECK_EQ(dimension_out.kind, ZarrDimensionType_Space);
    CHECK_EQ(dimension_out.array_size_px, 30);
    CHECK_EQ(dimension_out.chunk_size_px, 15);
    CHECK_EQ(dimension_out.shard_size_chunks, 4);

    dimension_out = ZarrStreamSettings_get_dimension(settings, 3);
    CHECK_EQ(std::string(dimension_out.name), "y");
    CHECK_EQ(dimension_out.kind, ZarrDimensionType_Space);
    CHECK_EQ(dimension_out.array_size_px, 20);
    CHECK_EQ(dimension_out.chunk_size_px, 10);
    CHECK_EQ(dimension_out.shard_size_chunks, 3);

    dimension_out = ZarrStreamSettings_get_dimension(settings, 4);
    CHECK_EQ(std::string(dimension_out.name), "x");
    CHECK_EQ(dimension_out.kind, ZarrDimensionType_Space);
    CHECK_EQ(dimension_out.array_size_px, 10);
    CHECK_EQ(dimension_out.chunk_size_px, 5);
    CHECK_EQ(dimension_out.shard_size_chunks, 2);

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
    ZarrStreamSettings* settings = ZarrStreamSettings_create();
    if (!settings) {
        fprintf(stderr, "Failed to create Zarr settings\n");
        return 1;
    }

    int retval = 0;

    CHECK(check_preconditions(settings));
    CHECK(set_and_get_parameters(settings));

Finalize:
    ZarrStreamSettings_destroy(settings);
    return retval;
Error:
    retval = 1;
    goto Finalize;
}