#pragma once

#include <cstddef> // size_t
#include <cstdint> // uint8_t
#include <string>
#include <vector>

struct ZarrDimension_s
{
    std::string name; /* Name of the dimension */
    uint8_t kind;     /* Type of dimension */

    uint32_t array_size_px;     /* Size of the array along this dimension */
    uint32_t chunk_size_px;     /* Size of a chunk along this dimension */
    uint32_t shard_size_chunks; /* Number of chunks in a shard along this
                                 dimension */
};

struct ZarrStreamSettings_s
{
    std::string store_path; /* Path to the Zarr store on the local filesystem */

    std::string s3_endpoint;          /* Endpoint for the S3 service */
    std::string s3_bucket_name;       /* Name of the S3 bucket */
    std::string s3_access_key_id;     /* Access key ID for the S3 service */
    std::string s3_secret_access_key; /* Secret access key for the S3 service */

    std::string external_metadata; /* JSON formatted external metadata for the
                                      base array */

    uint8_t dtype; /* Data type of the base array */

    uint8_t compressor;          /* Compression library to use */
    uint8_t compression_codec;   /* Compression codec to use */
    uint8_t compression_level;   /* Compression level to use */
    uint8_t compression_shuffle; /* Whether and how to shuffle the data before
                                  compressing */

    std::vector<ZarrDimension_s> dimensions; /* Dimensions of the base array */

    bool multiscale; /* Whether to stream to multiple resolutions */
};

bool
validate_dimension(const struct ZarrDimension_s& dimension);
