#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrStreamSettings* settings)
{
    ZarrStreamSettings_reserve_dimensions(settings, 3);

    ZarrDimensionProperties dim = {
        .name = "t",
        .bytes_of_name = sizeof("t"),
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
    };
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 0, &dim),
              ZarrStatus_Success);

    dim.name = "y";
    dim.type = ZarrDimensionType_Space;
    dim.array_size_px = 200;
    dim.chunk_size_px = 20;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 1, &dim),
              ZarrStatus_Success);

    dim.name = "x";
    dim.array_size_px = 300;
    dim.chunk_size_px = 30;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 2, &dim),
              ZarrStatus_Success);
}

void
compare_settings(const ZarrStreamSettings* settings,
                 const ZarrStreamSettings* settings_copy)
{
    EXPECT_STR_EQ(settings->store_path.c_str(),
                  settings_copy->store_path.c_str());

    EXPECT_STR_EQ(settings->s3_endpoint.c_str(),
                  settings_copy->s3_endpoint.c_str());
    EXPECT_STR_EQ(settings->s3_bucket_name.c_str(),
                  settings_copy->s3_bucket_name.c_str());
    EXPECT_STR_EQ(settings->s3_access_key_id.c_str(),
                  settings_copy->s3_access_key_id.c_str());
    EXPECT_STR_EQ(settings->s3_secret_access_key.c_str(),
                  settings_copy->s3_secret_access_key.c_str());

    EXPECT_STR_EQ(settings->custom_metadata.c_str(),
                  settings_copy->custom_metadata.c_str());

    EXPECT_EQ(int, "%d", settings->dtype, settings_copy->dtype);

    EXPECT_EQ(int, "%d", settings->compressor, settings_copy->compressor);
    EXPECT_EQ(int, "%d", settings->compression_codec, settings_copy->compression_codec);
    EXPECT_EQ(int, "%d", settings->compression_level, settings_copy->compression_level);
    EXPECT_EQ(int, "%d", settings->compression_shuffle, settings_copy->compression_shuffle);

    EXPECT_EQ(int, "%d", settings->dimensions.size(), 3);
    for (auto i = 0; i < 3; ++i) {
        const auto& dim = &settings->dimensions[i];
        const auto& dim_copy = &settings_copy->dimensions[i];
        EXPECT_STR_EQ(dim->name.c_str(), dim_copy->name.c_str());
        EXPECT_EQ(int, "%d", dim->type, dim_copy->type);
        EXPECT_EQ(int, "%d", dim->array_size_px, dim_copy->array_size_px);
        EXPECT_EQ(int, "%d", dim->chunk_size_px, dim_copy->chunk_size_px);
        EXPECT_EQ(int, "%d", dim->shard_size_chunks, dim_copy->shard_size_chunks);
    }

    EXPECT_EQ(bool, "%d", settings->multiscale, settings_copy->multiscale);
}

int
main()
{
    int retval = 1;

    ZarrStream* stream;
    ZarrStreamSettings *settings = ZarrStreamSettings_create(), *settings_copy;
    if (!settings) {
        LOG_ERROR("Failed to create ZarrStreamSettings");
        return retval;
    }
    settings->store_path = TEST ".zarr";

    try {
        configure_stream_dimensions(settings);
        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings->store_path));

        EXPECT_EQ(int, "%d", ZarrStream_get_version(stream), ZarrVersion_2);

        settings_copy = ZarrStream_get_settings(stream);
        CHECK(nullptr != settings_copy);
        CHECK(settings != settings_copy);
        compare_settings(settings, settings_copy);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR("%s", exception.what());
    }

    // cleanup
    if (fs::is_directory(settings->store_path)) {
        fs::remove_all(settings->store_path);
    }
    ZarrStreamSettings_destroy(settings);
    ZarrStreamSettings_destroy(settings_copy);
    ZarrStream_destroy(stream);

    return retval;
}