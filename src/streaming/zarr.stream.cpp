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
    if (s == nullptr || *s == '\0') {
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

bool
is_empty_string(const char* s, std::string_view error_msg)
{
    auto trimmed = trim(s);
    if (trimmed.empty()) {
        LOG_ERROR(error_msg);
        return true;
    }
    return false;
}

[[nodiscard]]
bool
validate_s3_settings(const ZarrS3Settings* settings)
{
    if (is_empty_string(settings->endpoint, "S3 endpoint is empty")) {
        return false;
    }
    if (is_empty_string(settings->access_key_id, "S3 access key ID is empty")) {
        return false;
    }
    if (is_empty_string(settings->secret_access_key,
                        "S3 secret access key is empty")) {
        return false;
    }

    std::string trimmed = trim(settings->bucket_name);
    if (trimmed.length() < 3 || trimmed.length() > 63) {
        LOG_ERROR("Invalid length for S3 bucket name: ",
                  trimmed.length(),
                  ". Must be between 3 "
                  "and 63 characters");
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
        LOG_ERROR("Parent path '",
                  parent_path,
                  "' does not exist or is not a directory");
        return false;
    }

    // parent path must be writable
    const auto perms = fs::status(parent_path).permissions();
    const bool is_writable =
      (perms & (fs::perms::owner_write | fs::perms::group_write |
                fs::perms::others_write)) != fs::perms::none;

    if (!is_writable) {
        LOG_ERROR("Parent path '", parent_path, "' is not writable");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_compression_settings(const ZarrCompressionSettings* settings)
{
    if (settings->compressor >= ZarrCompressorCount) {
        LOG_ERROR("Invalid compressor: ", settings->compressor);
        return false;
    }

    if (settings->codec >= ZarrCompressionCodecCount) {
        LOG_ERROR("Invalid compression codec: ", settings->codec);
        return false;
    }

    // if compressing, we require a compression codec
    if (settings->compressor != ZarrCompressor_None &&
        settings->codec == ZarrCompressionCodec_None) {
        LOG_ERROR("Compression codec must be set when using a compressor");
        return false;
    }

    if (settings->level > 9) {
        LOG_ERROR("Invalid compression level: ",
                  settings->level,
                  ". Must be between 0 and 9");
        return false;
    }

    if (settings->shuffle != BLOSC_NOSHUFFLE &&
        settings->shuffle != BLOSC_SHUFFLE &&
        settings->shuffle != BLOSC_BITSHUFFLE) {
        LOG_ERROR("Invalid shuffle: ",
                  settings->shuffle,
                  ". Must be ",
                  BLOSC_NOSHUFFLE,
                  " (no shuffle), ",
                  BLOSC_SHUFFLE,
                  " (byte  shuffle), or ",
                  BLOSC_BITSHUFFLE,
                  " (bit shuffle)");
        return false;
    }

    return true;
}

[[nodiscard]]
bool
validate_custom_metadata(const char* metadata)
{
    if (metadata == nullptr || !*metadata) {
        return true; // custom metadata is optional
    }

    // parse the JSON
    auto val = nlohmann::json::parse(metadata,
                                     nullptr, // callback
                                     false,   // allow exceptions
                                     true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: ", metadata);
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
    if (is_empty_string(dimension->name, "Dimension name is empty")) {
        return false;
    }

    if (dimension->type >= ZarrDimensionTypeCount) {
        LOG_ERROR("Invalid dimension type: ", dimension->type);
        return false;
    }

    if (!is_append && dimension->array_size_px == 0) {
        LOG_ERROR("Array size must be nonzero");
        return false;
    }

    if (dimension->chunk_size_px == 0) {
        LOG_ERROR("Invalid chunk size: ", dimension->chunk_size_px);
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
        LOG_ERROR("Invalid Zarr version: ", version);
        return false;
    }

    if (settings->store_path == nullptr) {
        LOG_ERROR("Null pointer: store_path");
        return false;
    }
    std::string_view store_path(settings->store_path);

    // we require the store path (root of the dataset) to be nonempty
    if (store_path.empty()) {
        LOG_ERROR("Store path is empty");
        return false;
    }

    if ((is_s3_acquisition(settings) &&
         !validate_s3_settings(settings->s3_settings)) ||
        (!is_s3_acquisition(settings) &&
         !validate_filesystem_store_path(store_path))) {
        return false;
    }

    if (settings->data_type >= ZarrDataTypeCount) {
        LOG_ERROR("Invalid data type: ", settings->data_type);
        return false;
    }

    if (is_compressed_acquisition(settings) &&
        !validate_compression_settings(settings->compression_settings)) {
        return false;
    }

    if (!validate_custom_metadata(settings->custom_metadata)) {
        return false;
    }

    if (settings->dimensions == nullptr) {
        LOG_ERROR("Null pointer: dimensions");
        return false;
    }

    // we must have at least 3 dimensions
    const size_t ndims = settings->dimension_count;
    if (ndims < 3) {
        LOG_ERROR("Invalid number of dimensions: ", ndims, ". Must be at least 3");
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
    EXPECT(create_store_(), error_);

    // allocate writers
    EXPECT(create_writers_(), error_);

    // allocate multiscale frame placeholders
    create_scaled_frames_();

    // allocate metadata sinks
    EXPECT(create_metadata_sinks_(), error_);

    // write base metadata
    EXPECT(write_base_metadata_(), error_);

    // write group metadata
    EXPECT(write_group_metadata_(), error_);

    // write external metadata
    EXPECT(write_external_metadata_(), error_);
}

ZarrStream_s::~ZarrStream_s()
{
    try {
        // must precede close of chunk file
        write_group_metadata_();
    } catch (const std::exception& e) {
        LOG_ERROR("Error finalizing Zarr stream: ", e.what());
    }
}

size_t
ZarrStream::append(const void* data, size_t nbytes)
{
    // TODO (aliddell): implement this
    return 0;
}

bool
ZarrStream_s::is_s3_acquisition_() const
{
    return s3_settings_.has_value();
}

bool
ZarrStream_s::is_compressed_acquisition_() const
{
    return compression_settings_.has_value();
}

void
ZarrStream_s::commit_settings_(const struct ZarrStreamSettings_s* settings)
{
    version_ = settings->version;
    store_path_ = trim(settings->store_path);
    custom_metadata_ = trim(settings->custom_metadata);

    if (is_s3_acquisition(settings)) {
        s3_settings_ = {
            .endpoint = trim(settings->s3_settings->endpoint),
            .bucket_name = trim(settings->s3_settings->bucket_name),
            .access_key_id = trim(settings->s3_settings->access_key_id),
            .secret_access_key = trim(settings->s3_settings->secret_access_key),
        };
    }

    if (is_compressed_acquisition(settings)) {
        compression_settings_ = {
            .compressor = settings->compression_settings->compressor,
            .codec = settings->compression_settings->codec,
            .level = settings->compression_settings->level,
            .shuffle = settings->compression_settings->shuffle,
        };
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
    if (is_s3_acquisition_()) {
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
