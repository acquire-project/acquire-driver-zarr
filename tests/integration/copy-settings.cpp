#include "zarr.h"
#include "test.logger.hh"

#include <cstring>
#include <stdexcept>

void
test_ZarrStreamSettings_copy()
{
    // Create original settings
    ZarrStreamSettings* original = ZarrStreamSettings_create();
    CHECK(original != nullptr);

    // Set various parameters
    const char* store_path = "/path/to/store";
    const char* s3_endpoint = "https://s3.amazonaws.com";
    const char* s3_bucket_name = "my-bucket";
    const char* s3_access_key_id = "access_key_123";
    const char* s3_secret_access_key = "secret_key_456";
    const char* custom_metadata = "{\"key\":\"value\"}";

    ZarrS3Settings s3_settings = {
        .endpoint = s3_endpoint,
        .bytes_of_endpoint = strlen(s3_endpoint) + 1,
        .bucket_name = s3_bucket_name,
        .bytes_of_bucket_name = strlen(s3_bucket_name) + 1,
        .access_key_id = s3_access_key_id,
        .bytes_of_access_key_id = strlen(s3_access_key_id) + 1,
        .secret_access_key = s3_secret_access_key,
        .bytes_of_secret_access_key = strlen(s3_secret_access_key) + 1,
    };

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_store(
                original, store_path, strlen(store_path) + 1, &s3_settings));

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_custom_metadata(
                original, custom_metadata, strlen(custom_metadata) + 1));

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_data_type(original, ZarrDataType_float32));

    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 5,
        .shuffle = 1,
    };

    EXPECT_EQ(
      ZarrStatus,
      "%d",
      ZarrStatus_Success,
      ZarrStreamSettings_set_compression(original, &compression_settings));

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_reserve_dimensions(original, 3));

    ZarrDimensionProperties dimension = {
        .name = "z",
        .bytes_of_name = strlen("z") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
    };
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(original, 0, &dimension));

    dimension = {
        .name = "y",
        .bytes_of_name = strlen("y") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
        .shard_size_chunks = 1,
    };
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(original, 1, &dimension));

    dimension = {
        .name = "x",
        .bytes_of_name = strlen("x") + 1,
        .kind = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
        .shard_size_chunks = 1,
    };
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(original, 2, &dimension));

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_multiscale(original, 1));

    // Copy the settings
    ZarrStreamSettings* copy = ZarrStreamSettings_copy(original);
    CHECK(copy != nullptr);

    // Verify copied settings
    EXPECT_STR_EQ(store_path, ZarrStreamSettings_get_store_path(copy));

    ZarrS3Settings s3_settings_copy = ZarrStreamSettings_get_s3_settings(copy);
    EXPECT_STR_EQ(s3_endpoint, s3_settings_copy.endpoint);
    EXPECT_STR_EQ(s3_bucket_name, s3_settings_copy.bucket_name);
    EXPECT_STR_EQ(s3_access_key_id, s3_settings_copy.access_key_id);
    EXPECT_STR_EQ(s3_secret_access_key, s3_settings_copy.secret_access_key);

    EXPECT_STR_EQ(custom_metadata,
                  ZarrStreamSettings_get_custom_metadata(copy));

    EXPECT_EQ(ZarrDataType,
              "%d",
              ZarrDataType_float32,
              ZarrStreamSettings_get_data_type(copy));

    ZarrCompressionSettings compression_settings_copy =
      ZarrStreamSettings_get_compression(copy);
    EXPECT_EQ(ZarrCompressor,
              "%d",
              ZarrCompressor_Blosc1,
              compression_settings_copy.compressor);
    EXPECT_EQ(ZarrCompressionCodec,
              "%d",
              ZarrCompressionCodec_BloscLZ4,
              compression_settings_copy.codec);
    EXPECT_EQ(uint8_t, "%u", 5, compression_settings_copy.level);
    EXPECT_EQ(uint8_t, "%u", 1, compression_settings_copy.shuffle);

    EXPECT_EQ(size_t, "%zu", 3, ZarrStreamSettings_get_dimension_count(copy));

    // Check dimensions
    for (size_t i = 0; i < 3; ++i) {
        auto dim = ZarrStreamSettings_get_dimension(copy, i);

        const char* expected_name = (i == 0) ? "z" : (i == 1) ? "y" : "x";
        EXPECT_STR_EQ(expected_name, dim.name);
        EXPECT_EQ(ZarrDimensionType, "%d", ZarrDimensionType_Space, dim.kind);
        EXPECT_EQ(size_t, "%zu", (i + 1) * 100, dim.array_size_px);
        EXPECT_EQ(size_t, "%zu", (i + 1) * 10, dim.chunk_size_px);
        EXPECT_EQ(size_t, "%zu", 1, dim.shard_size_chunks);
    }

    EXPECT_EQ(uint8_t, "%u", 1, ZarrStreamSettings_get_multiscale(copy));

    // Clean up
    ZarrStreamSettings_destroy(original);
    ZarrStreamSettings_destroy(copy);
}

int
main()
{
    try {
        test_ZarrStreamSettings_copy();
    } catch (const std::exception& e) {
        return 1;
    }

    return 0;
}