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
    return nullptr != settings->s3_settings;
}

bool
is_compressed_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return nullptr != settings->compression_settings;
}

[[nodiscard]]
std::string
trim(const char* s)
{
    if (!s || !*s) {
        return {};
    }

    const size_t length = strlen(s);

    // trim left
    std::string trimmed(s, length);
    trimmed.erase(trimmed.begin(),
                  std::find_if(trimmed.begin(), trimmed.end(), [](char c) {
                      return !std::isspace(c);
                  }));

    // trim right
    trimmed.erase(std::find_if(trimmed.rbegin(),
                               trimmed.rend(),
                               [](char c) { return !std::isspace(c); })
                    .base(),
                  trimmed.end());

    return trimmed;
}

[[nodiscard]]
bool
validate_s3_settings(const ZarrS3Settings* settings)
{
    std::string trimmed = trim(settings->endpoint);
    if (trimmed.empty()) {
        LOG_ERROR("S3 endpoint is empty");
        return false;
    }

    trimmed = trim(settings->bucket_name);
    if (trimmed.length() < 3 || trimmed.length() > 63) {
        LOG_ERROR("Invalid length for S3 bucket name: %zu. Must be between 3 "
                  "and 63 characters",
                  trimmed.length());
        return false;
    }

    trimmed = trim(settings->access_key_id);
    if (trimmed.empty()) {
        LOG_ERROR("S3 access key ID is empty");
        return false;
    }

    trimmed = trim(settings->secret_access_key);
    if (trimmed.empty()) {
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
validate_compression_settings(const ZarrCompressionSettings* settings)
{
    if (settings->compressor >= ZarrCompressorCount) {
        LOG_ERROR("Invalid compressor: %d", settings->compressor);
        return false;
    }

    if (settings->codec >= ZarrCompressionCodecCount) {
        LOG_ERROR("Invalid compression codec: %d", settings->codec);
        return false;
    }

    // if compressing, we require a compression codec
    if (settings->compressor != ZarrCompressor_None &&
        settings->codec == ZarrCompressionCodec_None) {
        LOG_ERROR("Compression codec must be set when using a compressor");
        return false;
    }

    if (settings->level > 9) {
        LOG_ERROR("Invalid compression level: %d. Must be between 0 and 9",
                  settings->level);
        return false;
    }

    if (settings->shuffle != BLOSC_NOSHUFFLE &&
        settings->shuffle != BLOSC_SHUFFLE &&
        settings->shuffle != BLOSC_BITSHUFFLE) {
        LOG_ERROR("Invalid shuffle: %d. Must be %d (no shuffle), %d (byte "
                  "shuffle), or %d (bit shuffle)",
                  settings->shuffle,
                  BLOSC_NOSHUFFLE,
                  BLOSC_SHUFFLE,
                  BLOSC_BITSHUFFLE);
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_custom_metadata(std::string_view metadata)
{
    if (metadata.empty()) {
        return true; // custom metadata is optional
    }

    // parse the JSON
    auto val = nlohmann::json::parse(metadata,
                                     nullptr, // callback
                                     false,   // allow exceptions
                                     true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: %s", metadata.data());
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_dimension(const ZarrDimensionProperties* dimension,
                   ZarrVersion version,
                   bool is_append)
{
    std::string trimmed = trim(dimension->name);
    if (trimmed.empty()) {
        LOG_ERROR("Invalid name. Must not be empty");
        return false;
    }

    if (dimension->type >= ZarrDimensionTypeCount) {
        LOG_ERROR("Invalid dimension type: %d", dimension->type);
        return false;
    }

    if (!is_append && dimension->array_size_px == 0) {
        LOG_ERROR("Array size must be nonzero");
        return false;
    }

    if (dimension->chunk_size_px == 0) {
        LOG_ERROR("Invalid chunk size: %zu", dimension->chunk_size_px);
        return false;
    }

    if (version == ZarrVersion_3 && dimension->shard_size_chunks == 0) {
        LOG_ERROR("Shard size must be nonzero");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_settings(const struct ZarrStreamSettings_s* settings)
{
    if (!settings) {
        LOG_ERROR("Null pointer: settings");
        return false;
    }

    auto version = settings->version;
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

    if (is_s3_acquisition(settings) &&
        !validate_s3_settings(settings->s3_settings)) {
        return false;
    } else if (!is_s3_acquisition(settings) &&
               !validate_filesystem_store_path(store_path)) {
        return false;
    }

    if (settings->data_type >= ZarrDataTypeCount) {
        LOG_ERROR("Invalid data type: %d", settings->data_type);
        return false;
    }

    if (is_compressed_acquisition(settings) &&
        !validate_compression_settings(settings->compression_settings)) {
        return false;
    }

    if (!validate_custom_metadata(settings->custom_metadata)) {
        return false;
    }

    if (nullptr == settings->dimensions) {
        LOG_ERROR("Null pointer: dimensions");
        return false;
    }

    // we must have at least 3 dimensions
    const size_t ndims = settings->dimension_count;
    if (ndims < 3) {
        LOG_ERROR("Invalid number of dimensions: %zu. Must be at least 3",
                  ndims);
        return false;
    }

    // check the final dimension (width), must be space
    if (settings->dimensions[ndims - 1].type != ZarrDimensionType_Space) {
        LOG_ERROR("Last dimension must be of type Space");
        return false;
    }

    // check the penultimate dimension (height), must be space
    if (settings->dimensions[ndims - 2].type != ZarrDimensionType_Space) {
        LOG_ERROR("Second to last dimension must be of type Space");
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < ndims; ++i) {
        if (!validate_dimension(settings->dimensions + i, version, i == 0)) {
            return false;
        }
    }

    return true;
}
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings)
  : error_()
{
    if (!validate_settings(settings)) {
        throw std::runtime_error("Invalid Zarr stream settings");
    }

    commit_settings_(settings);

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
ZarrStream_s::commit_settings_(const struct ZarrStreamSettings_s* settings)
{
    version_ = settings->version;
    store_path_ = trim(settings->store_path);
    custom_metadata_ = trim(settings->custom_metadata);

    if (is_s3_acquisition(settings)) {
        s3_endpoint_ = trim(settings->s3_settings->endpoint);
        s3_bucket_name_ = trim(settings->s3_settings->bucket_name);
        s3_access_key_id_ = trim(settings->s3_settings->access_key_id);
        s3_secret_access_key_ = trim(settings->s3_settings->secret_access_key);
        is_s3_acquisition_ = true;
    }

    if (is_compressed_acquisition(settings)) {
        compressor_ = settings->compression_settings->compressor;
        compression_codec_ = settings->compression_settings->codec;
        compression_level_ = settings->compression_settings->level;
        compression_shuffle_ = settings->compression_settings->shuffle;
        is_compressed_acquisition_ = true;
    }

    dtype_ = settings->data_type;

    for (auto i = 0; i < settings->dimension_count; ++i) {
        const auto& dim = settings->dimensions[i];
        dimensions_.emplace_back(dim.name,
                                 dim.type,
                                 dim.array_size_px,
                                 dim.chunk_size_px,
                                 dim.shard_size_chunks);
    }
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

bool
ZarrStream_s::create_store_()
{
    if (is_s3_acquisition_) {
        // TODO (aliddell): implement this
    } else {
        if (fs::exists(store_path_)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(store_path_, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           store_path_ + "': " + ec.message());
                return false;
            }
        }

        // create the store path
        {
            std::error_code ec;
            if (!fs::create_directories(store_path_, ec)) {
                set_error_("Failed to create store path '" +
                           store_path_ + "': " + ec.message());
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
    if (multiscale_) {
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
    if (multiscale_) {
        return;
    }

    // TODO (aliddell): implement this
}
