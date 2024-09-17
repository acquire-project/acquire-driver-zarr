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
get_store_path(ZarrStreamSettings* settings)
{
    EXPECT_STR_EQ(ZarrStreamSettings_get_store_path(settings), "");

    settings->store_path = TEST ".zarr";
    EXPECT_STR_EQ(ZarrStreamSettings_get_store_path(settings),
                  settings->store_path.c_str());
}

void
get_s3_settings(ZarrStreamSettings* settings)
{
    auto s3_settings = ZarrStreamSettings_get_s3_settings(settings);
    EXPECT_STR_EQ(s3_settings.endpoint, "");
    EXPECT_EQ(int, "%d", s3_settings.bytes_of_endpoint, 1);

    EXPECT_STR_EQ(s3_settings.bucket_name, "");
    EXPECT_EQ(int, "%d", s3_settings.bytes_of_bucket_name, 1);

    EXPECT_STR_EQ(s3_settings.access_key_id, "");
    EXPECT_EQ(int, "%d", s3_settings.bytes_of_access_key_id, 1);

    EXPECT_STR_EQ(s3_settings.secret_access_key, "");
    EXPECT_EQ(int, "%d", s3_settings.bytes_of_secret_access_key, 1);

    settings->s3_endpoint = "https://s3.amazonaws.com";
    settings->s3_bucket_name = "bucket";
    settings->s3_access_key_id = "access_key";
    settings->s3_secret_access_key = "secret_access_key";

    s3_settings = ZarrStreamSettings_get_s3_settings(settings);
    EXPECT_STR_EQ(s3_settings.endpoint, settings->s3_endpoint.c_str());
    EXPECT_EQ(int,
              "%d",
              s3_settings.bytes_of_endpoint,
              settings->s3_endpoint.size() + 1);

    EXPECT_STR_EQ(s3_settings.bucket_name, settings->s3_bucket_name.c_str());
    EXPECT_EQ(int,
              "%d",
              s3_settings.bytes_of_bucket_name,
              settings->s3_bucket_name.size() + 1);

    EXPECT_STR_EQ(s3_settings.access_key_id,
                  settings->s3_access_key_id.c_str());
    EXPECT_EQ(int,
              "%d",
              s3_settings.bytes_of_access_key_id,
              settings->s3_access_key_id.size() + 1);

    EXPECT_STR_EQ(s3_settings.secret_access_key,
                  settings->s3_secret_access_key.c_str());
    EXPECT_EQ(int,
              "%d",
              s3_settings.bytes_of_secret_access_key,
              settings->s3_secret_access_key.size() + 1);
}

void
get_compression(ZarrStreamSettings* settings)
{
    auto compression_settings = ZarrStreamSettings_get_compression(settings);

    EXPECT_EQ(int, "%d", compression_settings.compressor, ZarrCompressor_None);
    EXPECT_EQ(int, "%d", compression_settings.codec, ZarrCompressionCodec_None);
    EXPECT_EQ(int, "%d", compression_settings.level, 0);
    EXPECT_EQ(int, "%d", compression_settings.shuffle, 0);

    settings->compressor = ZarrCompressor_Blosc1;
    settings->compression_codec = ZarrCompressionCodec_BloscZstd;
    settings->compression_level = 8;
    settings->compression_shuffle = 2;

    compression_settings = ZarrStreamSettings_get_compression(settings);
    EXPECT_EQ(
      int, "%d", compression_settings.compressor, ZarrCompressor_Blosc1);
    EXPECT_EQ(
      int, "%d", compression_settings.codec, ZarrCompressionCodec_BloscZstd);
    EXPECT_EQ(int, "%d", compression_settings.level, 8);
    EXPECT_EQ(int, "%d", compression_settings.shuffle, 2);
}

void
get_data_type(ZarrStreamSettings* settings)
{
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_get_data_type(settings),
              ZarrDataType_uint8);

    settings->dtype = ZarrDataType_float32;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_get_data_type(settings),
              ZarrDataType_float32);
}

void
get_dimensions(ZarrStreamSettings* settings)
{
    EXPECT_EQ(int, "%d", ZarrStreamSettings_get_dimension_count(settings), 0);

    settings->dimensions.resize(3);
    EXPECT_EQ(int, "%d", ZarrStreamSettings_get_dimension_count(settings), 3);

    {
        auto& dim = settings->dimensions[0];
        dim.name = "time";
        dim.type = ZarrDimensionType_Time;
        dim.array_size_px = 100;
        dim.chunk_size_px = 13;
        dim.shard_size_chunks = 7;
    }
    {
        auto& dim = settings->dimensions[1];
        dim.name = "height";
        dim.array_size_px = 300;
        dim.chunk_size_px = 19;
        dim.shard_size_chunks = 13;
    }
    {
        auto& dim = settings->dimensions[2];
        dim.name = "width";
        dim.array_size_px = 200;
        dim.chunk_size_px = 17;
        dim.shard_size_chunks = 11;
    }

    // can't get beyond the last dimension
    auto dim = ZarrStreamSettings_get_dimension(settings, 3);
    EXPECT_STR_EQ(dim.name, "");
    EXPECT_EQ(int, "%d", dim.bytes_of_name, 0);
    EXPECT_EQ(int, "%d", dim.type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", dim.array_size_px, 0);
    EXPECT_EQ(int, "%d", dim.chunk_size_px, 0);
    EXPECT_EQ(int, "%d", dim.shard_size_chunks, 0);

    dim = ZarrStreamSettings_get_dimension(settings, 0);
    EXPECT_STR_EQ(dim.name, "time");
    EXPECT_EQ(int, "%d", dim.bytes_of_name, sizeof("time"));
    EXPECT_EQ(int, "%d", dim.type, ZarrDimensionType_Time);
    EXPECT_EQ(int, "%d", dim.array_size_px, 100);
    EXPECT_EQ(int, "%d", dim.chunk_size_px, 13);
    EXPECT_EQ(int, "%d", dim.shard_size_chunks, 7);

    dim = ZarrStreamSettings_get_dimension(settings, 1);
    EXPECT_STR_EQ(dim.name, "height");
    EXPECT_EQ(int, "%d", dim.type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", dim.bytes_of_name, sizeof("height"));
    EXPECT_EQ(int, "%d", dim.array_size_px, 300);
    EXPECT_EQ(int, "%d", dim.chunk_size_px, 19);
    EXPECT_EQ(int, "%d", dim.shard_size_chunks, 13);

    dim = ZarrStreamSettings_get_dimension(settings, 2);
    EXPECT_STR_EQ(dim.name, "width");
    EXPECT_EQ(int, "%d", dim.type, ZarrDimensionType_Space);
    EXPECT_EQ(int, "%d", dim.bytes_of_name, sizeof("width"));
    EXPECT_EQ(int, "%d", dim.array_size_px, 200);
    EXPECT_EQ(int, "%d", dim.chunk_size_px, 17);
    EXPECT_EQ(int, "%d", dim.shard_size_chunks, 11);
}

void
get_multiscale(ZarrStreamSettings* settings)
{
    CHECK(!ZarrStreamSettings_get_multiscale(settings));

    settings->multiscale = true;
    CHECK(ZarrStreamSettings_get_multiscale(settings));
}

void
get_custom_metadata(ZarrStreamSettings* settings)
{
    EXPECT_STR_EQ(settings->custom_metadata.c_str(), "{}");

    settings->custom_metadata = "this ain't even json"; // oops
    EXPECT_STR_EQ(settings->custom_metadata.c_str(), "this ain't even json");
}

int
main()
{
    int retval = 1;

    ZarrStreamSettings* settings = ZarrStreamSettings_create();
    try {
        CHECK(settings);
        check_preliminaries(settings);
        get_store_path(settings);
        get_s3_settings(settings);
        get_compression(settings);
        get_data_type(settings);
        get_dimensions(settings);
        get_multiscale(settings);
        get_custom_metadata(settings);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR("%s", exception.what());
    }
    ZarrStreamSettings_destroy(settings);
    return retval;
}