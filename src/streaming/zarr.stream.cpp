#include "macros.hh"
#include "zarr.stream.hh"
#include "acquire.zarr.h"

#include <blosc.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace {
bool
is_s3_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return !settings->s3_endpoint.empty() &&
           !settings->s3_bucket_name.empty() &&
           !settings->s3_access_key_id.empty() &&
           !settings->s3_secret_access_key.empty();
}

bool
is_compressed_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return settings->compressor != ZarrCompressor_None;
}

[[nodiscard]]
bool
validate_s3_settings(const struct ZarrStreamSettings_s* settings)
{
    if (settings->s3_endpoint.empty()) {
        LOG_ERROR("S3 endpoint is empty");
        return false;
    }
    if (settings->s3_bucket_name.length() < 3 ||
        settings->s3_bucket_name.length() > 63) {
        LOG_ERROR("Invalid length for S3 bucket name: %zu. Must be between 3 "
                  "and 63 characters",
                  settings->s3_bucket_name.length());
        return false;
    }
    if (settings->s3_access_key_id.empty()) {
        LOG_ERROR("S3 access key ID is empty");
        return false;
    }
    if (settings->s3_secret_access_key.empty()) {
        LOG_ERROR("S3 secret access key is empty");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_filesystem_store_path(std::string_view data_root)
{
    fs::path path(data_root);
    fs::path parent_path = path.parent_path();
    if (parent_path.empty()) {
        parent_path = ".";
    }

    // parent path must exist and be a directory
    if (!fs::exists(parent_path) || !fs::is_directory(parent_path)) {
        LOG_ERROR("Parent path '%s' does not exist or is not a directory",
                  parent_path.c_str());
        return false;
    }

    // parent path must be writable
    const auto perms = fs::status(parent_path).permissions();
    const bool is_writable =
      (perms & (fs::perms::owner_write | fs::perms::group_write |
                fs::perms::others_write)) != fs::perms::none;

    if (!is_writable) {
        LOG_ERROR("Parent path '%s' is not writable", parent_path.c_str());
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_compression_settings(const ZarrStreamSettings_s* settings)
{
    if (settings->compressor >= ZarrCompressorCount) {
        LOG_ERROR("Invalid compressor: %d", settings->compressor);
        return false;
    }

    if (settings->compression_codec >= ZarrCompressionCodecCount) {
        LOG_ERROR("Invalid compression codec: %d", settings->compression_codec);
        return false;
    }

    // we know the compressor is not None, so the codec must be set
    if (settings->compression_codec == ZarrCompressionCodec_None) {
        LOG_ERROR("Compression codec must be set when using a compressor");
        return false;
    }

    if (settings->compression_level == 0 || settings->compression_level > 9) {
        LOG_ERROR("Invalid compression level: %d. Must be between 1 and 9",
                  settings->compression_level);
        return false;
    }

    if (settings->compression_shuffle != BLOSC_NOSHUFFLE &&
        settings->compression_shuffle != BLOSC_SHUFFLE &&
        settings->compression_shuffle != BLOSC_BITSHUFFLE) {
        LOG_ERROR("Invalid shuffle: %d. Must be %d (no shuffle), %d (byte "
                  "shuffle), or %d (bit shuffle)",
                  settings->compression_shuffle,
                  BLOSC_NOSHUFFLE,
                  BLOSC_SHUFFLE,
                  BLOSC_BITSHUFFLE);
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_dimension(const struct ZarrDimension_s& dimension,
                   ZarrVersion version,
                   bool is_append)
{
    if (dimension.name.empty()) {
        LOG_ERROR("Invalid name. Must not be empty");
        return false;
    }

    if (dimension.type >= ZarrDimensionTypeCount) {
        LOG_ERROR("Invalid dimension type: %d", dimension.type);
        return false;
    }

    if (!is_append && dimension.array_size_px == 0) {
        LOG_ERROR("Array size must be nonzero");
        return false;
    }

    if (dimension.chunk_size_px == 0) {
        LOG_ERROR("Chunk size must be nonzero");
        return false;
    }

    if (version == ZarrVersion_3 && dimension.shard_size_chunks == 0) {
        LOG_ERROR("Shard size must be nonzero");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_settings(const struct ZarrStreamSettings_s* settings,
                  ZarrVersion version)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return false;
    }
    if (version < ZarrVersion_2 || version >= ZarrVersionCount) {
        LOG_ERROR("Invalid Zarr version: %d", version);
        return false;
    }

    std::string_view store_path(settings->store_path);

    // we require the store path (root of the dataset) to be nonempty
    if (store_path.empty()) {
        LOG_ERROR("Store path is empty");
        return false;
    }

    if (is_s3_acquisition(settings) && !validate_s3_settings(settings)) {
        return false;
    } else if (!is_s3_acquisition(settings) &&
               !validate_filesystem_store_path(store_path)) {
        return false;
    }

    if (settings->dtype >= ZarrDataTypeCount) {
        LOG_ERROR("Invalid data type: %d", settings->dtype);
        return false;
    }

    if (is_compressed_acquisition(settings) &&
        !validate_compression_settings(settings)) {
        return false;
    }

    // we must have at least 3 dimensions
    if (settings->dimensions.size() < 3) {
        LOG_ERROR("Invalid number of dimensions: %zu. Must be at least 3",
                  settings->dimensions.size());
        return false;
    }

    // check the final dimension (width), must be space
    if (settings->dimensions.back().type != ZarrDimensionType_Space) {
        LOG_ERROR("Last dimension must be of type Space");
        return false;
    }

    // check the penultimate dimension (height), must be space
    if (settings->dimensions[settings->dimensions.size() - 2].type !=
        ZarrDimensionType_Space) {
        LOG_ERROR("Second to last dimension must be of type Space");
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < settings->dimensions.size(); ++i) {
        if (!validate_dimension(settings->dimensions[i], version, i == 0)) {
            return false;
        }
    }

    return true;
}
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings,
                         ZarrVersion version)
  : version_(version)
  , error_()
{
    if (!validate_settings(settings, version)) {
        throw std::runtime_error("Invalid Zarr stream settings");
    }

    // create the data store
    EXPECT(create_store_(), "%s", error_.c_str());

    // allocate writers
    EXPECT(create_writers_(), "%s", error_.c_str());

    // allocate multiscale frame placeholders
    create_scaled_frames_();

    // allocate metadata sinks
    EXPECT(create_metadata_sinks_(), "%s", error_.c_str());

    // write base metadata
    EXPECT(write_base_metadata_(), "%s", error_.c_str());

    // write group metadata
    EXPECT(write_group_metadata_(), "%s", error_.c_str());

    // write external metadata
    EXPECT(write_external_metadata_(), "%s", error_.c_str());
}

ZarrStream_s::~ZarrStream_s()
{
    try {
        // must precede close of chunk file
        write_group_metadata_();
    } catch (const std::exception& e) {
        LOG_ERROR("Error finalizing Zarr stream: %s", e.what());
    }
}

size_t
ZarrStream::append(const void* data, size_t nbytes)
{
    // TODO (aliddell): implement this
    return 0;
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

bool
ZarrStream_s::create_store_()
{
    if (is_s3_acquisition(&settings_)) {
        // TODO (aliddell): implement this
    } else {
        if (fs::exists(settings_.store_path_)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(settings_.store_path_, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           settings_.store_path_ + "': " + ec.message());
                return false;
            }
        }

        // create the store path
        {
            std::error_code ec;
            if (!fs::create_directories(settings_.store_path_, ec)) {
                set_error_("Failed to create store path '" +
                           settings_.store_path_ + "': " + ec.message());
                return false;
            }
        }
    }

    return true;
}

bool
ZarrStream_s::create_writers_()
{
    // TODO (aliddell): implement this
    return true;
}

void
ZarrStream_s::create_scaled_frames_()
{
    if (settings_.multiscale) {
        // TODO (aliddell): implement this
    }
}

bool
ZarrStream_s::create_metadata_sinks_()
{
    // TODO (aliddell): implement this
    return true;
}

bool
ZarrStream_s::write_base_metadata_()
{
    // TODO (aliddell): implement this
    return true;
}

bool
ZarrStream_s::write_group_metadata_()
{
    // TODO (aliddell): implement this
    return true;
}

bool
ZarrStream_s::write_external_metadata_()
{
    // TODO (aliddell): implement this
    return true;
}

nlohmann::json
ZarrStream_s::make_multiscale_metadata_() const
{
    // TODO (aliddell): implement this
    return {};
}

void
ZarrStream_s::write_multiscale_frames_(const uint8_t* data,
                                       size_t bytes_of_data)
{
    if (!settings_.multiscale) {
        return;
    }

    // TODO (aliddell): implement this
}
