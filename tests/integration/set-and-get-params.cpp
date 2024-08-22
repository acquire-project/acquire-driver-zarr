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

#define TRY_SET_STRING(stream, member, value)                                  \
    do {                                                                       \
        ZarrError err;                                                         \
        if (err = ZarrStreamSettings_set_##member(                             \
              stream, value, strlen(value) + 1);                               \
            err != ZarrError_Success) {                                        \
            fprintf(stderr,                                                    \
                    "Failed to set %s: %s\n",                                  \
                    #member,                                                   \
                    Zarr_get_error_message(err));                              \
            return false;                                                      \
        }                                                                      \
    } while (0)

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
set_and_get_parameters(ZarrStreamSettings* stream)
{
    std::string str_param_value;

    /* Set and get store path */
    TRY_SET_STRING(stream, store_path, "store_path");
    str_param_value = ZarrStreamSettings_get_store_path(stream);
    CHECK_EQ(str_param_value, "store_path");

    /* Set and get S3 endpoint */
    TRY_SET_STRING(stream, s3_endpoint, "s3_endpoint");
    str_param_value = ZarrStreamSettings_get_s3_endpoint(stream);
    CHECK_EQ(str_param_value, "s3_endpoint");

    /* Set and get S3 bucket name */
    TRY_SET_STRING(stream, s3_bucket_name, "s3_bucket_name");
    str_param_value = ZarrStreamSettings_get_s3_bucket_name(stream);
    CHECK_EQ(str_param_value, "s3_bucket_name");

    /* Set and get S3 access key ID */
    TRY_SET_STRING(stream, s3_access_key_id, "s3_access_key_id");
    str_param_value = ZarrStreamSettings_get_s3_access_key_id(stream);
    CHECK_EQ(str_param_value, "s3_access_key_id");

    /* Set and get S3 secret access key */
    TRY_SET_STRING(stream, s3_secret_access_key, "s3_secret_access_key");
    str_param_value = ZarrStreamSettings_get_s3_secret_access_key(stream);
    CHECK_EQ(str_param_value, "s3_secret_access_key");

    /* Set and get compressor */
    CHECK_EQ(ZarrStreamSettings_set_compressor(stream, ZarrCompressor_Blosc1),
             ZarrError_Success);

    // try to set an invalid compressor
    CHECK_EQ(ZarrStreamSettings_set_compressor(stream, ZarrCompressor_Blosc2),
             ZarrError_NotYetImplemented);

    // should still be the previous value
    CHECK_EQ(ZarrStreamSettings_get_compressor(stream), ZarrCompressor_Blosc1);

    /* Set and get compression codec */
    CHECK_EQ(ZarrStreamSettings_set_compression_codec(
               stream, ZarrCompressionCodec_BloscLZ4),
             ZarrError_Success);

    /* Set and get some dimensions */
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               stream, 4, SIZED("x"), ZarrDimensionType_Space, 10, 5, 2),
             ZarrError_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               stream, 3, SIZED("y"), ZarrDimensionType_Space, 20, 10, 3),
             ZarrError_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               stream, 2, SIZED("z"), ZarrDimensionType_Space, 30, 15, 4),
             ZarrError_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               stream, 1, SIZED("c"), ZarrDimensionType_Channel, 40, 20, 5),
             ZarrError_Success);
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               stream, 0, SIZED("t"), ZarrDimensionType_Time, 50, 25, 6),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_get_dimension_count(stream), 5);

    char name[64];
    ZarrDimensionType kind;
    size_t array_size_px;
    size_t chunk_size_px;
    size_t shard_size_chunks;

    CHECK_EQ(ZarrStreamSettings_get_dimension(stream,
                                              0,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrError_Success);
    CHECK_EQ(std::string(name), "t");
    CHECK_EQ(kind, ZarrDimensionType_Time);
    CHECK_EQ(array_size_px, 50);
    CHECK_EQ(chunk_size_px, 25);
    CHECK_EQ(shard_size_chunks, 6);

    CHECK_EQ(ZarrStreamSettings_get_dimension(stream,
                                              1,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrError_Success);
    CHECK_EQ(std::string(name), "c");
    CHECK_EQ(kind, ZarrDimensionType_Channel);
    CHECK_EQ(array_size_px, 40);
    CHECK_EQ(chunk_size_px, 20);
    CHECK_EQ(shard_size_chunks, 5);

    CHECK_EQ(ZarrStreamSettings_get_dimension(stream,
                                              2,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrError_Success);
    CHECK_EQ(std::string(name), "z");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 30);
    CHECK_EQ(chunk_size_px, 15);
    CHECK_EQ(shard_size_chunks, 4);

    CHECK_EQ(ZarrStreamSettings_get_dimension(stream,
                                              3,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrError_Success);
    CHECK_EQ(std::string(name), "y");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 20);
    CHECK_EQ(chunk_size_px, 10);
    CHECK_EQ(shard_size_chunks, 3);

    CHECK_EQ(ZarrStreamSettings_get_dimension(stream,
                                              4,
                                              name,
                                              64,
                                              &kind,
                                              &array_size_px,
                                              &chunk_size_px,
                                              &shard_size_chunks),
             ZarrError_Success);
    CHECK_EQ(std::string(name), "x");
    CHECK_EQ(kind, ZarrDimensionType_Space);
    CHECK_EQ(array_size_px, 10);
    CHECK_EQ(chunk_size_px, 5);
    CHECK_EQ(shard_size_chunks, 2);

    /* Set and get multiscale */
    CHECK_EQ(ZarrStreamSettings_set_multiscale(stream, 10), ZarrError_Success);
    CHECK_EQ(ZarrStreamSettings_get_multiscale(stream), 1); // normalized to 1

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