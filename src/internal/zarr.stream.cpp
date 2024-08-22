#include <filesystem>
#include <string_view>

#include "zarr.stream.hh"
#include "zarr.h"

namespace fs = std::filesystem;

namespace {
bool
validate_settings(struct ZarrStreamSettings_s* settings, ZarrVersion version)
{
    if (!settings || version < ZarrVersion_2 || version >= ZarrVersionCount)
        return false;

    // validate the dimensions individually
    for (size_t i = 0; i < settings->dimensions.size(); ++i) {
        if (!validate_dimension(settings->dimensions[i]))
            return false;

        if (i > 0 && settings->dimensions[i].array_size_px == 0)
            return false;
    }

    // if version 3, we require shard size to be positive
    if (version == ZarrVersion_3) {
        for (const auto& dim : settings->dimensions) {
            if (dim.shard_size_chunks == 0)
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

    // initialize the stream
    ZarrStream_s* stream;

    try {
        stream = new ZarrStream(settings, version);
    } catch (const std::bad_alloc&) {
        return nullptr;
    } catch (const std::exception& e) {
        fprintf(stderr, "Error creating Zarr stream: %s\n", e.what());
        return nullptr;
    }
    ZarrStreamSettings_destroy(settings);

    return stream;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    delete stream;
}

/** ZarrStream_s implementation **/

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings, size_t version)
  : settings_(*settings)
  , version_(version)
  , error_()
{
    settings_.dimensions = std::move(settings->dimensions);

    // spin up thread pool
    thread_pool_ = std::make_unique<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error_(err); });

    // create the store if it doesn't exist
    create_store_();

    if (!error_.empty())
        throw std::runtime_error("Error creating Zarr stream: " + error_);
}

ZarrStream_s::~ZarrStream_s()
{
    thread_pool_->await_stop();
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

void
ZarrStream_s::create_store_()
{
    // if the store path is empty, we're using S3
    if (settings_.store_path.empty()) {
        // create the S3 store
        // ...
    } else {
        if (fs::exists(settings_.store_path)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(settings_.store_path, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           settings_.store_path + "': " + ec.message());
                return;
            }
        }

        // create the store path
        fs::create_directories(settings_.store_path);
    }
}