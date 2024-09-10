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
    const char* external_metadata = "{\"key\":\"value\"}";

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
              ZarrStreamSettings_set_external_metadata(
                original, external_metadata, strlen(external_metadata) + 1));

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
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(
                original, 0, "z", 2, ZarrDimensionType_Space, 100, 10, 1));
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(
                original, 1, "y", 2, ZarrDimensionType_Space, 200, 20, 1));
    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_dimension(
                original, 2, "x", 2, ZarrDimensionType_Space, 300, 30, 1));

    EXPECT_EQ(ZarrStatus,
              "%d",
              ZarrStatus_Success,
              ZarrStreamSettings_set_multiscale(original, 1));

    // Copy the settings
    ZarrStreamSettings* copy = ZarrStreamSettings_copy(original);
    CHECK(copy != nullptr);

    // Verify copied settings
    EXPECT_STR_EQ(store_path, ZarrStreamSettings_get_store_path(copy));
    EXPECT_STR_EQ(s3_endpoint, ZarrStreamSettings_get_s3_endpoint(copy));
    EXPECT_STR_EQ(s3_bucket_name, ZarrStreamSettings_get_s3_bucket_name(copy));
    EXPECT_STR_EQ(s3_access_key_id,
                  ZarrStreamSettings_get_s3_access_key_id(copy));
    EXPECT_STR_EQ(s3_secret_access_key,
                  ZarrStreamSettings_get_s3_secret_access_key(copy));
    EXPECT_STR_EQ(external_metadata,
                  ZarrStreamSettings_get_external_metadata(copy));

    EXPECT_EQ(ZarrDataType,
              "%d",
              ZarrDataType_float32,
              ZarrStreamSettings_get_data_type(copy));
    EXPECT_EQ(ZarrCompressor,
              "%d",
              ZarrCompressor_Blosc1,
              ZarrStreamSettings_get_compressor(copy));
    EXPECT_EQ(ZarrCompressionCodec,
              "%d",
              ZarrCompressionCodec_BloscLZ4,
              ZarrStreamSettings_get_compression_codec(copy));
    EXPECT_EQ(uint8_t, "%u", 5, ZarrStreamSettings_get_compression_level(copy));
    EXPECT_EQ(
      uint8_t, "%u", 1, ZarrStreamSettings_get_compression_shuffle(copy));

    EXPECT_EQ(size_t, "%zu", 3, ZarrStreamSettings_get_dimension_count(copy));

    // Check dimensions
    for (size_t i = 0; i < 3; ++i) {
        char name[3];
        size_t bytes_of_name = sizeof(name);
        ZarrDimensionType kind;
        size_t array_size_px, chunk_size_px, shard_size_chunks;

        EXPECT_EQ(ZarrStatus,
                  "%d",
                  ZarrStatus_Success,
                  ZarrStreamSettings_get_dimension(copy,
                                                   i,
                                                   name,
                                                   bytes_of_name,
                                                   &kind,
                                                   &array_size_px,
                                                   &chunk_size_px,
                                                   &shard_size_chunks));

        const char* expected_name = (i == 0) ? "z" : (i == 1) ? "y" : "x";
        EXPECT_STR_EQ(expected_name, name);
        EXPECT_EQ(ZarrDimensionType, "%d", ZarrDimensionType_Space, kind);
        EXPECT_EQ(size_t, "%zu", (i + 1) * 100, array_size_px);
        EXPECT_EQ(size_t, "%zu", (i + 1) * 10, chunk_size_px);
        EXPECT_EQ(size_t, "%zu", 1, shard_size_chunks);
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