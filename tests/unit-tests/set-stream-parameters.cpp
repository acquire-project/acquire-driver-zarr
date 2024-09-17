#include "acquire.zarr.h"
#include "stream.settings.hh"
#include "unit.test.macros.hh"

void
check_preliminaries(ZarrStreamSettings* settings)
{
    CHECK(settings);

    CHECK(settings->store_path.empty());

    CHECK(settings->s3_endpoint.empty());
    CHECK(settings->s3_bucket_name.empty());
    CHECK(settings->s3_access_key_id.empty());
    CHECK(settings->s3_secret_access_key.empty());

    EXPECT_STR_EQ(settings->custom_metadata.c_str(), "{}");

    EXPECT_EQ(int, "%d", settings->dtype, ZarrDataType_uint8);

    EXPECT_EQ(int, "%d", settings->compressor, ZarrCompressor_None);
    EXPECT_EQ(
      int, "%d", settings->compression_codec, ZarrCompressionCodec_None);
    EXPECT_EQ(int, "%d", settings->compression_level, 0);
    EXPECT_EQ(int, "%d", settings->compression_shuffle, 0);

    CHECK(settings->dimensions.empty());

    CHECK(!settings->multiscale);
}

void
set_store(ZarrStreamSettings* settings)
{
    std::string store_path = TEST ".zarr";
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_store(
                settings, store_path.c_str(), store_path.size() + 1, nullptr),
              ZarrStatus_Success);

    EXPECT_STR_EQ(settings->store_path.c_str(), store_path.c_str());
    settings->store_path = ""; // reset

    ZarrS3Settings s3_settings{
        .endpoint = "https://s3.amazonaws.com",
        .bytes_of_endpoint = sizeof("https://s3.amazonaws.com"),
        .bucket_name = "bucket",
        .bytes_of_bucket_name = sizeof("bucket"),
        .access_key_id = "access_key",
        .bytes_of_access_key_id = sizeof("access_key"),
        .secret_access_key = "secret_access_key",
        .bytes_of_secret_access_key = sizeof("secret_access_key"),
    };

    EXPECT_EQ(
      int,
      "%d",
      ZarrStreamSettings_set_store(
        settings, store_path.c_str(), store_path.size() + 1, &s3_settings),
      ZarrStatus_Success);

    EXPECT_STR_EQ(settings->store_path.c_str(), store_path.c_str());
    EXPECT_STR_EQ(settings->s3_endpoint.c_str(), s3_settings.endpoint);
    EXPECT_STR_EQ(settings->s3_bucket_name.c_str(), s3_settings.bucket_name);
    EXPECT_STR_EQ(settings->s3_access_key_id.c_str(),
                  s3_settings.access_key_id);
    EXPECT_STR_EQ(settings->s3_secret_access_key.c_str(),
                  s3_settings.secret_access_key);
}

void
set_compression(ZarrStreamSettings* settings)
{
    ZarrCompressionSettings compression_settings{
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 5,
        .shuffle = 1,
    };

    EXPECT_EQ(
      int,
      "%d",
      ZarrStreamSettings_set_compression(settings, &compression_settings),
      ZarrStatus_Success);

    EXPECT_EQ(int, "%d", settings->compressor, ZarrCompressor_Blosc1);
    EXPECT_EQ(
      int, "%d", settings->compression_codec, ZarrCompressionCodec_BloscLZ4);
    EXPECT_EQ(int, "%d", settings->compression_level, 5);
    EXPECT_EQ(int, "%d", settings->compression_shuffle, 1);
}

void
set_data_type(ZarrStreamSettings* settings)
{
    ZarrDataType dtype = ZarrDataType_uint16;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_data_type(settings, dtype),
              ZarrStatus_Success);
    EXPECT_EQ(int, "%d", settings->dtype, ZarrDataType_uint16);
}

void
set_dimensions(ZarrStreamSettings* settings)
{
    ZarrStreamSettings_reserve_dimensions(settings, 3);
    EXPECT_EQ(int, "%d", settings->dimensions.size(), 3);

    ZarrDimensionProperties dim{
        .name = " time   ",
        .bytes_of_name = sizeof(" time   "),
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 13,
        .shard_size_chunks = 7,
    };

    // can't set a dimension that is out of bounds
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 3, &dim),
              ZarrStatus_InvalidIndex);

    // set the first dimension
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 0, &dim),
              ZarrStatus_Success);

    EXPECT_STR_EQ(settings->dimensions[0].name.c_str(), "time");
    EXPECT_EQ(int, "%d", settings->dimensions[0].type, ZarrDimensionType_Time);
    EXPECT_EQ(int, "%d", settings->dimensions[0].array_size_px, 100);
    EXPECT_EQ(int, "%d", settings->dimensions[0].chunk_size_px, 13);
    EXPECT_EQ(int, "%d", settings->dimensions[0].shard_size_chunks, 7);

    // other dimensions should still be unset
    EXPECT_STR_EQ(settings->dimensions[1].name.c_str(), "");
    EXPECT_EQ(int, "%d", settings->dimensions[1].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[1].array_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[1].chunk_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[1].shard_size_chunks, 0);

    EXPECT_STR_EQ(settings->dimensions[2].name.c_str(), "");
    EXPECT_EQ(int, "%d", settings->dimensions[2].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[2].array_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[2].chunk_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[2].shard_size_chunks, 0);

    // set the 3rd dimension before the 2nd
    dim.name = "width ";
    dim.bytes_of_name = sizeof("width ");
    dim.type = ZarrDimensionType_Space;
    dim.array_size_px = 200;
    dim.chunk_size_px = 17;
    dim.shard_size_chunks = 11;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 2, &dim),
              ZarrStatus_Success);

    EXPECT_STR_EQ(settings->dimensions[0].name.c_str(), "time");
    EXPECT_EQ(int, "%d", settings->dimensions[0].type, ZarrDimensionType_Time);
    EXPECT_EQ(int, "%d", settings->dimensions[0].array_size_px, 100);
    EXPECT_EQ(int, "%d", settings->dimensions[0].chunk_size_px, 13);
    EXPECT_EQ(int, "%d", settings->dimensions[0].shard_size_chunks, 7);

    // 2nd dimension should still be unset
    EXPECT_STR_EQ(settings->dimensions[1].name.c_str(), "");
    EXPECT_EQ(int, "%d", settings->dimensions[1].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[1].array_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[1].chunk_size_px, 0);
    EXPECT_EQ(int, "%d", settings->dimensions[1].shard_size_chunks, 0);

    EXPECT_STR_EQ(settings->dimensions[2].name.c_str(), "width");
    EXPECT_EQ(int, "%d", settings->dimensions[2].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[2].array_size_px, 200);
    EXPECT_EQ(int, "%d", settings->dimensions[2].chunk_size_px, 17);
    EXPECT_EQ(int, "%d", settings->dimensions[2].shard_size_chunks, 11);

    // set the 2nd dimension
    dim.name = "height";
    dim.bytes_of_name = sizeof("height");
    dim.type = ZarrDimensionType_Space;
    dim.array_size_px = 300;
    dim.chunk_size_px = 19;
    dim.shard_size_chunks = 13;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 1, &dim),
              ZarrStatus_Success);

    EXPECT_STR_EQ(settings->dimensions[0].name.c_str(), "time");
    EXPECT_EQ(int, "%d", settings->dimensions[0].type, ZarrDimensionType_Time);
    EXPECT_EQ(int, "%d", settings->dimensions[0].array_size_px, 100);
    EXPECT_EQ(int, "%d", settings->dimensions[0].chunk_size_px, 13);
    EXPECT_EQ(int, "%d", settings->dimensions[0].shard_size_chunks, 7);

    EXPECT_STR_EQ(settings->dimensions[1].name.c_str(), "height");
    EXPECT_EQ(int, "%d", settings->dimensions[1].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[1].array_size_px, 300);
    EXPECT_EQ(int, "%d", settings->dimensions[1].chunk_size_px, 19);
    EXPECT_EQ(int, "%d", settings->dimensions[1].shard_size_chunks, 13);

    EXPECT_STR_EQ(settings->dimensions[2].name.c_str(), "width");
    EXPECT_EQ(int, "%d", settings->dimensions[2].type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", settings->dimensions[2].array_size_px, 200);
    EXPECT_EQ(int, "%d", settings->dimensions[2].chunk_size_px, 17);
    EXPECT_EQ(int, "%d", settings->dimensions[2].shard_size_chunks, 11);
}

void
set_multiscale(ZarrStreamSettings* settings)
{
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_multiscale(settings, true),
              ZarrStatus_Success);
    CHECK(settings->multiscale);

    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_multiscale(settings, false),
              ZarrStatus_Success);
    CHECK(!settings->multiscale);
}

void
set_custom_metadata(ZarrStreamSettings* settings)
{
    // fails when not JSON formatted
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_custom_metadata(
                settings, "this is not json", sizeof("this is not json")),
              ZarrStatus_InvalidArgument);
    EXPECT_STR_EQ(settings->custom_metadata.c_str(), "{}");

    // succeeds when JSON formatted
    EXPECT_EQ(
      int,
      "%d",
      ZarrStreamSettings_set_custom_metadata(
        settings, "{\"key\": \"value\"}", sizeof("{\"key\": \"value\"}")),
      ZarrStatus_Success);
    // whitespace is removed
    EXPECT_STR_EQ(settings->custom_metadata.c_str(), "{\"key\":\"value\"}");
}

int
main()
{
    int retval = 1;

    ZarrStreamSettings* settings = ZarrStreamSettings_create();
    try {
        CHECK(settings);
        check_preliminaries(settings);
        set_store(settings);
        set_compression(settings);
        set_data_type(settings);
        set_dimensions(settings);
        set_multiscale(settings);
        set_custom_metadata(settings);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR("%s", exception.what());
    }
    ZarrStreamSettings_destroy(settings);
    return retval;
}