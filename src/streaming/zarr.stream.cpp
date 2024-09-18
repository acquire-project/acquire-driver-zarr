#include "macros.hh"
#include "zarr.stream.hh"
#include "acquire.zarr.h"
#include "zarr.common.hh"
#include "zarrv2.array.writer.hh"
#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"

#include <blosc.h>

#include <filesystem>

#ifdef min
#undef min
#endif

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

const char*
dimension_type_to_string(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Time:
            return "time";
        case ZarrDimensionType_Channel:
            return "channel";
        case ZarrDimensionType_Space:
            return "space";
        case ZarrDimensionType_Other:
            return "other";
        default:
            return "(unknown)";
    }
}

template<typename T>
[[nodiscard]]
uint8_t*
scale_image(const uint8_t* const src,
            size_t& bytes_of_src,
            size_t& width,
            size_t& height)
{
    CHECK(src);

    const size_t bytes_of_frame = width * height * sizeof(T);
    EXPECT(bytes_of_src >= bytes_of_frame,
           "Expecting at least %zu bytes, got %zu",
           bytes_of_frame,
           bytes_of_src);

    const int downscale = 2;
    constexpr size_t bytes_of_type = sizeof(T);
    const double factor = 0.25;

    const auto w_pad = width + (width % downscale);
    const auto h_pad = height + (height % downscale);

    const auto size_downscaled =
      static_cast<uint32_t>(w_pad * h_pad * factor * bytes_of_type);

    auto* dst = new uint8_t[size_downscaled];
    EXPECT(dst,
           "Failed to allocate %zu bytes for destination frame",
           size_downscaled);

    memset(dst, 0, size_downscaled);

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            size_t src_idx = row * width + col;
            const bool pad_width = (col == width - 1 && width != w_pad);

            double here = src[src_idx];
            double right = src[src_idx + (1 - static_cast<int>(pad_width))];
            double down =
              src[src_idx + width * (1 - static_cast<int>(pad_height))];
            double diag =
              src[src_idx + width * (1 - static_cast<int>(pad_height)) +
                  (1 - static_cast<int>(pad_width))];

            dst[dst_idx++] =
              static_cast<T>(factor * (here + right + down + diag));
        }
    }

    bytes_of_src = size_downscaled;
    width = w_pad / 2;
    height = h_pad / 2;

    return dst;
}

template<typename T>
void
average_two_frames(void* dst_,
                   size_t bytes_of_dst,
                   const void* src_,
                   size_t bytes_of_src)
{
    CHECK(dst_);
    CHECK(src_);
    EXPECT(bytes_of_dst == bytes_of_src,
           "Expecting %zu bytes in destination, got %zu",
           bytes_of_src,
           bytes_of_dst);

    T* dst = static_cast<T*>(dst_);
    const T* src = static_cast<const T*>(src_);

    const auto num_pixels = bytes_of_src / sizeof(T);
    for (auto i = 0; i < num_pixels; ++i) {
        dst[i] = static_cast<T>(0.5 * (dst[i] + src[i]));
    }
}
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings,
                         ZarrVersion version)
  : settings_(*settings)
  , version_(version)
  , error_()
  , frame_buffer_offset_(0)
{
    // spin up thread pool
    thread_pool_ = std::make_shared<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error_(err); });

    // allocate a frame buffer
    frame_buffer_.resize(zarr::bytes_of_frame(
      settings_.dimensions, static_cast<ZarrDataType>(settings_.dtype)));

    // create the data store
    EXPECT(create_store_(), "%s", error_.c_str());

    // allocate writers
    EXPECT(create_writers_(), "%s", error_.c_str());

    // allocate multiscale frame placeholders
    if (settings_.multiscale) {
        create_scaled_frames_();
    }

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
    if (!write_group_metadata_()) {
        LOG_ERROR("Error finalizing Zarr stream: %s", error_.c_str());
    }
    metadata_sinks_.clear();

    writers_.clear(); // flush before shutting down thread pool
    thread_pool_->await_stop();

    for (auto& [_, frame] : scaled_frames_) {
        if (frame) {
            delete[] *frame;
        }
    }
}

size_t
ZarrStream::append(const void* data_, size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: %s", error_.c_str());

    if (0 == nbytes) {
        return 0;
    }

    auto* data = static_cast<const uint8_t*>(data_);

    const size_t bytes_of_frame = frame_buffer_.size();
    size_t bytes_written = 0;

    while (bytes_written < nbytes) {
        const size_t bytes_remaining = nbytes - bytes_written;
        if (frame_buffer_offset_ > 0) { // add to / finish a partial frame
            const size_t bytes_to_copy =
              std::min(bytes_of_frame - frame_buffer_offset_, bytes_remaining);

            memcpy(frame_buffer_.data() + frame_buffer_offset_,
                   data + bytes_written,
                   bytes_to_copy);
            frame_buffer_offset_ += bytes_to_copy;
            bytes_written += bytes_to_copy;

            // ready to flush the frame buffer
            if (frame_buffer_offset_ == bytes_of_frame) {
                const size_t bytes_written_this_frame =
                  writers_[0]->write_frame(data, bytes_of_frame);
                if (bytes_written_this_frame == 0) {
                    break;
                }

                bytes_written += bytes_to_copy;
                data += bytes_to_copy;
                frame_buffer_offset_ = 0;
            }
        } else if (bytes_remaining < bytes_of_frame) { // begin partial frame
            memcpy(frame_buffer_.data(), data, bytes_remaining);
            frame_buffer_offset_ = bytes_remaining;
            bytes_written += bytes_remaining;
        } else { // at least one full frame
            const size_t bytes_written_this_frame =
              writers_[0]->write_frame(data, bytes_of_frame);
            if (bytes_written_this_frame == 0) {
                break;
            }

            write_multiscale_frames_(data, bytes_written_this_frame);

            bytes_written += bytes_written_this_frame;
            data += bytes_written_this_frame;
        }
    }

    return bytes_written;
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
        // spin up S3 connection pool
        try {
            s3_connection_pool_ = std::make_shared<zarr::S3ConnectionPool>(
              std::thread::hardware_concurrency(),
              settings_.s3_endpoint,
              settings_.s3_access_key_id,
              settings_.s3_secret_access_key);
        } catch (const std::exception& e) {
            set_error_("Error creating S3 connection pool: " +
                       std::string(e.what()));
            return false;
        }

        // test the S3 connection
        auto conn = s3_connection_pool_->get_connection();
        if (!conn->check_connection()) {
            set_error_("Failed to connect to S3");
            return false;
        }
        s3_connection_pool_->return_connection(std::move(conn));
    } else {
        if (fs::exists(settings_.store_path)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(settings_.store_path, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           settings_.store_path + "': " + ec.message());
                return false;
            }
        }

        // create the store path
        {
            std::error_code ec;
            if (!fs::create_directories(settings_.store_path, ec)) {
                set_error_("Failed to create store path '" +
                           settings_.store_path + "': " + ec.message());
                return false;
            }
        }
    }

    return true;
}

bool
ZarrStream_s::create_writers_()
{
    writers_.clear();

    // construct Blosc compression parameters
    std::optional<zarr::BloscCompressionParams> blosc_compression_params;
    if (settings_.compressor == ZarrCompressor_Blosc1) {
        blosc_compression_params = zarr::BloscCompressionParams(
          zarr::blosc_codec_to_string(
            static_cast<ZarrCompressionCodec>(settings_.compression_codec)),
          settings_.compression_level,
          settings_.compression_shuffle);
    }

    std::optional<std::string> bucket_name;
    if (is_s3_acquisition(&settings_)) {
        bucket_name = settings_.s3_bucket_name;
    }

    zarr::ArrayWriterConfig config = {
        .dimensions = settings_.dimensions,
        .dtype = static_cast<ZarrDataType>(settings_.dtype),
        .level_of_detail = 0,
        .bucket_name = bucket_name,
        .store_path = settings_.store_path,
        .compression_params = blosc_compression_params
    };

    if (version_ == 2) {
        writers_.push_back(std::make_unique<zarr::ZarrV2ArrayWriter>(
          config, thread_pool_, s3_connection_pool_));
    } else {
        writers_.push_back(std::make_unique<zarr::ZarrV3ArrayWriter>(
          config, thread_pool_, s3_connection_pool_));
    }

    if (settings_.multiscale) {
        zarr::ArrayWriterConfig downsampled_config;

        bool do_downsample = true;
        while (do_downsample) {
            do_downsample = downsample(config, downsampled_config);

            if (version_ == 2) {
                writers_.push_back(std::make_unique<zarr::ZarrV2ArrayWriter>(
                  downsampled_config, thread_pool_, s3_connection_pool_));
            } else {
                writers_.push_back(std::make_unique<zarr::ZarrV3ArrayWriter>(
                  downsampled_config, thread_pool_, s3_connection_pool_));
            }
            //            scaled_frames_.emplace(level++, std::nullopt);

            config = std::move(downsampled_config);
            downsampled_config = {};
        }
    }

    return true;
}

void
ZarrStream_s::create_scaled_frames_()
{
    for (size_t level = 1; level < writers_.size(); ++level) {
        scaled_frames_.emplace(level, std::nullopt);
    }
}

bool
ZarrStream_s::create_metadata_sinks_()
{
    zarr::SinkCreator creator(thread_pool_, s3_connection_pool_);

    try {
        if (s3_connection_pool_) {
            if (!creator.make_metadata_sinks(version_,
                                             settings_.s3_bucket_name,
                                             settings_.store_path,
                                             metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        } else {
            if (!creator.make_metadata_sinks(
                  version_, settings_.store_path, metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        }
    } catch (const std::exception& e) {
        set_error_("Error creating metadata sinks: " + std::string(e.what()));
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_base_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata["multiscales"] = make_multiscale_metadata_();

        metadata_key = ".zattrs";
    } else {
        metadata["extensions"] = nlohmann::json::array();
        metadata["metadata_encoding"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";
        metadata["metadata_key_suffix"] = ".json";
        metadata["zarr_format"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";

        metadata_key = "zarr.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();

    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing base metadata");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_group_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata = { { "zarr_format", 2 } };

        metadata_key = ".zgroup";
    } else {
        metadata["attributes"]["multiscales"] = make_multiscale_metadata_();

        metadata_key = "meta/root.group.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing group metadata");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_external_metadata_()
{
    if (settings_.custom_metadata.empty()) {
        return true;
    }

    auto metadata = nlohmann::json::parse(settings_.custom_metadata,
                                          nullptr, // callback
                                          false,   // allow exceptions
                                          true     // ignore comments
    );
    std::string metadata_key = "acquire.json";

    if (version_ == 3) {
        metadata_key = "meta/" + metadata_key;
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();
    if (!sink->write(0, metadata_bytes, metadata_str.size())) {
        set_error_("Error writing external metadata");
        return false;
    }

    return true;
}

nlohmann::json
ZarrStream_s::make_multiscale_metadata_() const
{
    nlohmann::json multiscales;
    const auto& dimensions = settings_.dimensions;
    multiscales[0]["version"] = "0.4";

    auto& axes = multiscales[0]["axes"];
    for (auto dim = dimensions.begin(); dim != dimensions.end(); ++dim) {
        std::string type = dimension_type_to_string(dim->type);

        if (dim < dimensions.end() - 2) {
            axes.push_back({ { "name", dim->name.c_str() }, { "type", type } });
        } else {
            axes.push_back({ { "name", dim->name.c_str() },
                             { "type", type },
                             { "unit", "micrometer" } });
        }
    }

    // spatial multiscale metadata
    std::vector<double> scales(dimensions.size(), 1.0);
    multiscales[0]["datasets"] = {
        {
          { "path", "0" },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        },
    };

    for (auto i = 1; i < writers_.size(); ++i) {
        scales.clear();
        scales.push_back(std::pow(2, i)); // append
        for (auto k = 0; k < dimensions.size() - 3; ++k) {
            scales.push_back(1.);
        }
        scales.push_back(std::pow(2, i)); // y
        scales.push_back(std::pow(2, i)); // x

        multiscales[0]["datasets"].push_back({
          { "path", std::to_string(i) },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        });

        // downsampling metadata
        multiscales[0]["type"] = "local_mean";
        multiscales[0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters "
              "are "
              "given here." },
            { "method", "skimage.transform.downscale_local_mean" },
            { "version", "0.21.0" },
            { "args", "[2]" },
            { "kwargs", { "cval", 0 } },
        };
    }

    return multiscales;
}

void
ZarrStream_s::write_multiscale_frames_(const uint8_t* data,
                                       size_t bytes_of_data)
{
    if (!settings_.multiscale) {
        return;
    }

    std::function<uint8_t*(const uint8_t*, size_t&, size_t&, size_t&)> scale;
    std::function<void(void*, size_t, const void*, size_t)> average2;

    auto dtype = static_cast<ZarrDataType>(settings_.dtype);
    switch (dtype) {
        case ZarrDataType_uint8:
            scale = scale_image<uint8_t>;
            average2 = average_two_frames<uint8_t>;
            break;
        case ZarrDataType_uint16:
            scale = scale_image<uint16_t>;
            average2 = average_two_frames<uint16_t>;
            break;
        case ZarrDataType_uint32:
            scale = scale_image<uint32_t>;
            average2 = average_two_frames<uint32_t>;
            break;
        case ZarrDataType_uint64:
            scale = scale_image<uint64_t>;
            average2 = average_two_frames<uint64_t>;
            break;
        case ZarrDataType_int8:
            scale = scale_image<int8_t>;
            average2 = average_two_frames<int8_t>;
            break;
        case ZarrDataType_int16:
            scale = scale_image<int16_t>;
            average2 = average_two_frames<int16_t>;
            break;
        case ZarrDataType_int32:
            scale = scale_image<int32_t>;
            average2 = average_two_frames<int32_t>;
            break;
        case ZarrDataType_int64:
            scale = scale_image<int64_t>;
            average2 = average_two_frames<int64_t>;
            break;
        case ZarrDataType_float32:
            scale = scale_image<float>;
            average2 = average_two_frames<float>;
            break;
        case ZarrDataType_float64:
            scale = scale_image<double>;
            average2 = average_two_frames<double>;
            break;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(dtype));
    }

    const auto& dims = settings_.dimensions;
    size_t frame_width = dims.back().array_size_px;
    size_t frame_height = dims[dims.size() - 2].array_size_px;

    uint8_t* dst;
    for (auto i = 1; i < writers_.size(); ++i) {
        dst = scale(data, bytes_of_data, frame_width, frame_height);

        // bytes_of data is now downscaled
        // frame_width and frame_height are now the new dimensions

        if (scaled_frames_[i]) {
            average2(dst, bytes_of_data, *scaled_frames_[i], bytes_of_data);
            EXPECT(writers_[i]->write_frame(dst, bytes_of_data),
                   "Failed to write frame to writer %zu",
                   i);

            // clean up this LOD
            delete[] *scaled_frames_[i];
            scaled_frames_[i].reset();

            // set up for next iteration
            if (i + 1 < writers_.size()) {
                data = dst;
            } else {
                delete[] dst;
            }
        } else {
            scaled_frames_[i] = dst;
            break;
        }
    }
}

// C API
extern "C"
{
    ZarrStream* ZarrStream_create(struct ZarrStreamSettings_s* settings,
                                  ZarrVersion version)
    {
        if (!validate_settings(settings, version)) {
            return nullptr;
        }

        // initialize the stream
        ZarrStream_s* stream = nullptr;

        try {
            stream = new ZarrStream(settings, version);
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for Zarr stream");
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating Zarr stream: %s", e.what());
        }

        return stream;
    }

    void ZarrStream_destroy(ZarrStream* stream)
    {
        delete stream;
    }

    ZarrStatus ZarrStream_append(ZarrStream* stream,
                                 const void* data,
                                 size_t bytes_in,
                                 size_t* bytes_out)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(data, "Null pointer: data");
        EXPECT_VALID_ARGUMENT(bytes_out, "Null pointer: bytes_out");

        try {
            *bytes_out = stream->append(data, bytes_in);
        } catch (const std::exception& e) {
            LOG_ERROR("Error appending data: %s", e.what());
            return ZarrStatus_InternalError;
        }

        return ZarrStatus_Success;
    }

    ZarrVersion ZarrStream_get_version(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning ZarrVersion_2");
            return ZarrVersion_2;
        }

        return stream->version();
    }

    ZarrStreamSettings* ZarrStream_get_settings(const ZarrStream* stream)
    {
        if (!stream) {
            LOG_WARNING("Null pointer: stream. Returning nullptr");
            return nullptr;
        }

        return ZarrStreamSettings_copy(&stream->settings());
    }
}