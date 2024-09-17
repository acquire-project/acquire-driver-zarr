#pragma once

#include "zarr.types.h"

#include <cstddef> // size_t
#include <cstdint> // uint8_t
#include <string>
#include <vector>

struct ZarrDimension_s
{
    std::string name;       /* Name of the dimension */
    ZarrDimensionType type; /* Type of dimension */

    uint32_t array_size_px;     /* Size of the array along this dimension */
    uint32_t chunk_size_px;     /* Size of a chunk along this dimension */
    uint32_t shard_size_chunks; /* Number of chunks in a shard along this
                                 dimension */
};

struct ZarrStreamSettings_s
{
  public:
    ZarrStreamSettings_s();
    std::string store_path; /* Path to the Zarr store on the local filesystem */

    std::string s3_endpoint;          /* Endpoint for the S3 service */
    std::string s3_bucket_name;       /* Name of the S3 bucket */
    std::string s3_access_key_id;     /* Access key ID for the S3 service */
    std::string s3_secret_access_key; /* Secret access key for the S3 service */

    std::string custom_metadata; /* JSON formatted external metadata for the
                                      base array */

    ZarrDataType dtype; /* Data type of the base array */

    ZarrCompressor compressor;              /* Compression library to use */
    ZarrCompressionCodec compression_codec; /* Compression codec to use */
    uint8_t compression_level;              /* Compression level to use */
    uint8_t compression_shuffle; /* Whether and how to shuffle the data before
                                                        compressing */

    std::vector<ZarrDimension_s> dimensions; /* Dimensions of the base array */

    bool multiscale; /* Whether to stream to multiple resolutions */
};
