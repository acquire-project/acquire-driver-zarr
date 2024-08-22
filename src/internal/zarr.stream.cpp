#include <cstring>
#include <filesystem>
#include <string_view>

#include "zarr.stream.hh"
#include "zarr.h"

namespace fs = std::filesystem;

namespace {
ZarrError
ZarrStreamSettings_defrag_dims(
  struct ZarrStreamSettings_s::ZarrStream_dimensions_s* dimensions)
{
    if (!dimensions)
        return ZarrError_InvalidArgument;

    const size_t max_dims =
      sizeof(dimensions->data) / sizeof(dimensions->data[0]);
    size_t count = 0;
    size_t first_empty = 0;

    // move all non-empty slots to the front
    while (count < dimensions->count) {
        // search for the first empty slot
        for (size_t i = first_empty; i < max_dims; ++i) {
            if (dimensions->data[i].name[0] == '\0') {
                first_empty = i;
                break;
            }
        }

        struct ZarrDimension_s* empty = &dimensions->data[first_empty];

        // search for the first non-empty slot after the empty slot
        for (size_t i = first_empty + 1; i < max_dims; ++i) {
            if (dimensions->data[i].name[0] != '\0') {
                struct ZarrDimension_s* nonempty = &dimensions->data[i];
                memcpy(empty, nonempty, sizeof(*empty));
                memset(nonempty, 0, sizeof(*nonempty));
                ++first_empty;
                ++count;
                break;
            }
        }
    }

    // verify that the dimensions are contiguous
    for (size_t i = 0; i < dimensions->count; ++i) {
        if (dimensions->data[i].name[0] == '\0') {
            return ZarrError_Failure;
        }
    }
    for (size_t i = dimensions->count; i < max_dims; ++i) {
        if (dimensions->data[i].name[0] != '\0') {
            return ZarrError_Failure;
        }
    }

    return ZarrError_Success;
}

bool
validate_settings(struct ZarrStreamSettings_s* settings, ZarrVersion version)
{
    if (!settings || version < ZarrVersion_2 || version >= ZarrVersionCount)
        return false;

    // we require at least 3 dimensions
    if (settings->dimensions.count < 3)
        return false;

    // defrag the dimensions and validate them individually
    if (ZarrStreamSettings_defrag_dims(&settings->dimensions) !=
        ZarrError_Success)
        return false;

    for (size_t i = 0; i < settings->dimensions.count; ++i) {
        if (!validate_dimension(&settings->dimensions.data[i]))
            return false;
    }

    // all but the first dimension must have a non-zero array size
    for (size_t i = 1; i < settings->dimensions.count; ++i) {
        if (settings->dimensions.data[i].array_size_px == 0)
            return false;
    }

    // if version 3, we require shard size to be positive
    if (version == ZarrVersion_3) {
        for (size_t i = 0; i < settings->dimensions.count; ++i) {
            if (settings->dimensions.data[i].shard_size_chunks == 0)
                return false;
        }
    }

    std::string_view store_path(settings->store_path);
    std::string_view s3_endpoint(settings->s3_endpoint);
    std::string_view s3_bucket_name(settings->s3_bucket_name);
    std::string_view s3_access_key_id(settings->s3_access_key_id);
    std::string_view s3_secret_access_key(settings->s3_secret_access_key);

    // if the store path is empty, we require all S3 settings
    if (store_path.empty()) {
        if (s3_endpoint.empty() || s3_bucket_name.empty() ||
            s3_access_key_id.empty() || s3_secret_access_key.empty())
            return false;

        // check that the S3 endpoint is a valid URL
        if (s3_endpoint.find("http://") != 0 &&
            s3_endpoint.find("https://") != 0)
            return false;
    } else {
        // check that the store path either exists and is a valid directory
        // or that it can be created
        fs::path path(store_path);
        fs::path parent_path = path.parent_path();
        if (parent_path.empty())
            parent_path = ".";

        // parent path must exist and be a directory
        if (!fs::exists(parent_path) || !fs::is_directory(parent_path))
            return false;

        // parent path must be writable
        const auto perms = fs::status(parent_path).permissions();
        const bool is_writable =
          (perms & (fs::perms::owner_write | fs::perms::group_write |
                    fs::perms::others_write)) != fs::perms::none;

        if (!is_writable)
            return false;
    }

    return true;
}
} // namespace

ZarrStream*
ZarrStream_create(struct ZarrStreamSettings_s* settings, ZarrVersion version)
{
    if (!validate_settings(settings, version))
        return nullptr;

    auto* stream = new struct ZarrStream_s;

    memset(stream, 0, sizeof(*stream));
    stream->settings = settings;
    stream->version = version;

    return stream;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    if (!stream)
        return;

    ZarrStreamSettings_destroy(stream->settings);
    delete stream;
}