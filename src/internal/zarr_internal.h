#ifndef H_ZARR_INTERNAL
#define H_ZARR_INTERNAL

struct ZarrDimension_s
{
    char name[64]; /* Name of the dimension */
    size_t kind;   /* Type of dimension */

    size_t array_size_px; /* Size of the array along this dimension */
    size_t chunk_size_px; /* Size of a chunk along this dimension */
    size_t
      shard_size_chunks; /* Number of chunks in a shard along this dimension */
};

struct ZarrStreamSettings_s
{
    char store_path[1024]; /* Path to the Zarr store on the local filesystem */

    char s3_endpoint[256];          /* Endpoint for the S3 service */
    char s3_bucket_name[64];        /* Name of the S3 bucket */
    char s3_access_key_id[256];     /* Access key ID for the S3 service */
    char s3_secret_access_key[256]; /* Secret access key for the S3 service */

    size_t compressor; /* Compression library to use */
    size_t codec;      /* Compression codec to use */

    struct ZarrStream_dimensions_s
    {
        struct ZarrDimension_s data[64]; /* Array of dimensions */
        size_t count;                    /* Number of dimensions */
    } dimensions;
};

#endif // H_ZARR_INTERNAL
