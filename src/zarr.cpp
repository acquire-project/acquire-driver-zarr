#include "zarr.hh"

#include "writers/zarrv2.array.writer.hh"
#include "nlohmann/json.hpp"

#include <tuple> // std::ignore

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;
using json = nlohmann::json;

namespace {
/// \brief Get the filename from a StorageProperties as fs::path.
/// \param props StorageProperties for the Zarr Storage device.
/// \return fs::path representation of the Zarr data directory.
fs::path
as_path(const StorageProperties& props)
{
    if (!props.uri.str) {
        return {};
    }

    std::string uri{ props.uri.str, props.uri.nbytes - 1 };

    if (uri.find("file://") == std::string::npos) {
        return uri;
    }

    return uri.substr(7);
}

/// \brief Check that the JSON string is valid. (Valid can mean empty.)
/// \param str Putative JSON metadata string.
/// \param nbytes Size of the JSON metadata char array
void
validate_json(const char* str, size_t nbytes)
{
    // Empty strings are valid (no metadata is fine).
    if (nbytes <= 1 || nullptr == str) {
        return;
    }

    std::ignore = json::parse(str,
                              str + nbytes,
                              nullptr, // callback
                              true,    // allow exceptions
                              true     // ignore comments
    );
}

/// \brief Check that the StorageProperties are valid.
/// \details Assumes either an empty or valid JSON metadata string and a
/// filename string that points to a writable directory. \param props Storage
/// properties for Zarr. \throw std::runtime_error if the parent of the Zarr
/// data directory is not an existing directory.
void
validate_props(const StorageProperties* props)
{
    EXPECT(props->uri.str, "URI string is NULL.");
    EXPECT(props->uri.nbytes, "URI string is zero size.");

    // check that JSON is correct (throw std::exception if not)
    validate_json(props->external_metadata_json.str,
                  props->external_metadata_json.nbytes);

    std::string uri{ props->uri.str, props->uri.nbytes - 1 };

    if (common::is_web_uri(uri)) {
        std::vector<std::string> tokens = common::split_uri(uri);
        CHECK(tokens.size() > 2); // http://endpoint/bucket
    } else {
        const fs::path path = as_path(*props);
        fs::path parent_path = path.parent_path();
        if (parent_path.empty())
            parent_path = ".";

        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.string().c_str());

        // check directory is writable
        const auto perms = fs::status(fs::path(parent_path)).permissions();

        EXPECT((perms & (fs::perms::owner_write | fs::perms::group_write |
                         fs::perms::others_write)) != fs::perms::none,
               "Expected \"%s\" to have write permissions.",
               parent_path.c_str());
    }
}

void
validate_dimension(const zarr::Dimension& dim, bool is_append)
{
    if (is_append) {
        EXPECT(dim.array_size_px == 0,
               "Append dimension array size must be 0.");
    } else {
        EXPECT(dim.array_size_px > 0, "Dimension array size must be positive.");
    }

    EXPECT(dim.chunk_size_px > 0, "Dimension chunk size must be positive.");
}

[[nodiscard]] bool
is_multiscale_supported(const std::vector<zarr::Dimension>& dims)
{
    // 0. Must have at least 3 dimensions.
    if (dims.size() < 3) {
        return false;
    }

    // 1. The first two dimensions must be space dimensions.
    if (dims.at(0).kind != DimensionType_Space ||
        dims.at(1).kind != DimensionType_Space) {
        return false;
    }

    // 2. Interior dimensions must have size 1
    for (auto i = 2; i < dims.size() - 1; ++i) {
        if (dims.at(i).array_size_px != 1) {
            return false;
        }
    }

    return true;
}

template<typename T>
VideoFrame*
scale_image(const uint8_t* const data,
            size_t bytes_of_data,
            const struct ImageShape& shape)
{
    CHECK(data);
    CHECK(bytes_of_data);

    const int downscale = 2;
    constexpr size_t bytes_of_type = sizeof(T);
    const auto factor = 0.25f;

    const auto width = shape.dims.width;
    const auto w_pad = width + (width % downscale);

    const auto height = shape.dims.height;
    const auto h_pad = height + (height % downscale);

    const auto size_of_image =
      static_cast<uint32_t>(w_pad * h_pad * factor * bytes_of_type);

    const size_t bytes_of_frame =
      common::align_up(sizeof(VideoFrame) + size_of_image, 8);

    auto* dst = (VideoFrame*)malloc(bytes_of_frame);
    CHECK(dst);
    dst->bytes_of_frame = bytes_of_frame;

    {
        dst->shape = shape;
        dst->shape.dims = {
            .width = w_pad / downscale,
            .height = h_pad / downscale,
        };
        dst->shape.strides = {
            .height = dst->shape.dims.width,
            .planes = dst->shape.dims.width * dst->shape.dims.height,
        };

        CHECK(bytes_of_image(&dst->shape) == size_of_image);
    }

    const auto* src_img = (T*)data;
    auto* dst_img = (T*)dst->data;
    memset(dst_img, 0, size_of_image);

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            const bool pad_width = (col == width - 1 && width != w_pad);

            size_t idx = row * width + col;
            dst_img[dst_idx++] =
              (T)(factor *
                  ((float)src_img[idx] +
                   (float)src_img[idx + (1 - (int)pad_width)] +
                   (float)src_img[idx + width * (1 - (int)pad_height)] +
                   (float)src_img[idx + width * (1 - (int)pad_height) +
                                  (1 - (int)pad_width)]));
        }
    }

    return dst;
}

/// @brief Average both `dst` and `src` into `dst`.
template<typename T>
void
average_two_frames(VideoFrame* dst, const VideoFrame* src)
{
    CHECK(dst);
    CHECK(src);
    CHECK(dst->bytes_of_frame == src->bytes_of_frame);

    const auto nbytes_image = bytes_of_image(&dst->shape);
    const auto num_pixels = nbytes_image / sizeof(T);
    for (auto i = 0; i < num_pixels; ++i) {
        dst->data[i] = (T)(0.5f * ((float)dst->data[i] + (float)src->data[i]));
    }
}

DeviceState
zarr_set(Storage* self_, const StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        self->set(props);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
        return DeviceState_AwaitingConfiguration;
    } catch (...) {
        LOGE("Exception: (unknown)");
        return DeviceState_AwaitingConfiguration;
    }

    return DeviceState_Armed;
}

void
zarr_get(const Storage* self_, StorageProperties* props) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        self->get(props);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

void
zarr_get_meta(const Storage* self_, StoragePropertyMetadata* meta) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        self->get_meta(meta);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

DeviceState
zarr_start(Storage* self_) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        self->start();
        state = self->state;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return state;
}

DeviceState
zarr_append(Storage* self_, const VideoFrame* frames, size_t* nbytes) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        *nbytes = self->append(frames, *nbytes);
        state = self->state;
    } catch (const std::exception& exc) {
        *nbytes = 0;
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        *nbytes = 0;
        LOGE("Exception: (unknown)");
    }

    return state;
}

DeviceState
zarr_stop(Storage* self_) noexcept
{
    DeviceState state{ DeviceState_AwaitingConfiguration };

    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        CHECK(self->stop()); // state is set to DeviceState_Armed here
        state = self->state;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return state;
}

void
zarr_destroy(Storage* self_) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        if (self_->stop)
            self_->stop(self_);

        delete self;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}

void
zarr_reserve_image_shape(Storage* self_, const ImageShape* shape) noexcept
{
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        self->reserve_image_shape(shape);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }
}
} // end ::{anonymous} namespace

void
zarr::Zarr::set(const StorageProperties* props)
{
    EXPECT(state != DeviceState_Running,
           "Cannot set properties while running.");
    CHECK(props);

    // checks the directory exists and is writable
    validate_props(props);

    std::string uri(props->uri.str, props->uri.nbytes - 1);

    if (common::is_web_uri(uri)) {
        dataset_root_ = uri;
    } else {
        dataset_root_ = as_path(*props).string();
    }

    if (props->access_key_id.str) {
        s3_access_key_id_ = std::string(props->access_key_id.str,
                                        props->access_key_id.nbytes - 1);
    }

    if (props->secret_access_key.str) {
        s3_secret_access_key_ = std::string(
          props->secret_access_key.str, props->secret_access_key.nbytes - 1);
    }

    if (props->external_metadata_json.str) {
        external_metadata_json_ =
          std::string(props->external_metadata_json.str,
                      props->external_metadata_json.nbytes - 1);
    }

    pixel_scale_um_ = props->pixel_scale_um;

    set_dimensions_(props);
    enable_multiscale_ = props->enable_multiscale &&
                         is_multiscale_supported(acquisition_dimensions_);
}

void
zarr::Zarr::get(StorageProperties* props) const
{
    CHECK(props);
    storage_properties_destroy(props);

    std::string uri;
    if (common::is_web_uri(dataset_root_)) {
        uri = dataset_root_;
    } else if (!dataset_root_.empty()) {
        fs::path dataset_root_abs = fs::absolute(dataset_root_);
        uri = "file://" + dataset_root_abs.string();
    }

    const size_t bytes_of_filename = uri.empty() ? 0 : uri.size() + 1;

    const char* metadata = external_metadata_json_.empty()
                             ? nullptr
                             : external_metadata_json_.c_str();
    const size_t bytes_of_metadata =
      metadata ? external_metadata_json_.size() + 1 : 0;

    CHECK(storage_properties_init(props,
                                  0,
                                  uri.c_str(),
                                  bytes_of_filename,
                                  metadata,
                                  bytes_of_metadata,
                                  pixel_scale_um_,
                                  acquisition_dimensions_.size()));

    // set access key and secret
    {
        const char* access_key_id =
          s3_access_key_id_.has_value() ? s3_access_key_id_->c_str() : nullptr;
        const size_t bytes_of_access_key_id =
          access_key_id ? s3_access_key_id_->size() + 1 : 0;

        const char* secret_access_key = s3_secret_access_key_.has_value()
                                          ? s3_secret_access_key_->c_str()
                                          : nullptr;
        const size_t bytes_of_secret_access_key =
          secret_access_key ? s3_secret_access_key_->size() + 1 : 0;

        if (access_key_id && secret_access_key) {
            CHECK(storage_properties_set_access_key_and_secret(
              props,
              access_key_id,
              bytes_of_access_key_id,
              secret_access_key,
              bytes_of_secret_access_key));
        }
    }

    for (auto i = 0; i < acquisition_dimensions_.size(); ++i) {
        const auto dim = acquisition_dimensions_.at(i);
        CHECK(storage_properties_set_dimension(props,
                                               i,
                                               dim.name.c_str(),
                                               dim.name.length() + 1,
                                               dim.kind,
                                               dim.array_size_px,
                                               dim.chunk_size_px,
                                               dim.shard_size_chunks));
    }

    storage_properties_set_enable_multiscale(props,
                                             (uint8_t)enable_multiscale_);
}

void
zarr::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    memset(meta, 0, sizeof(*meta));

    meta->chunking_is_supported = 1;
    meta->multiscale_is_supported = 1;
    meta->s3_is_supported = 1;
}

void
zarr::Zarr::start()
{
    error_ = true;

    thread_pool_ = std::make_shared<common::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error(err); });

    if (common::is_web_uri(dataset_root_)) {
        std::vector<std::string> tokens = common::split_uri(dataset_root_);
        CHECK(tokens.size() > 1);
        const std::string endpoint = tokens[0] + "//" + tokens[1];
        connection_pool_ = std::make_shared<common::S3ConnectionPool>(
          8, endpoint, *s3_access_key_id_, *s3_secret_access_key_);
    } else {
        // remove the folder if it exists
        if (fs::exists(dataset_root_)) {
            std::error_code ec;
            EXPECT(fs::remove_all(dataset_root_, ec),
                   R"(Failed to remove folder for "%s": %s)",
                   dataset_root_.c_str(),
                   ec.message().c_str());
        }

        // create the dataset folder
        fs::create_directories(dataset_root_);
    }

    allocate_writers_();

    make_metadata_sinks_();
    write_fixed_metadata_();

    state = DeviceState_Running;
    error_ = false;
}

int
zarr::Zarr::stop() noexcept
{
    int is_ok = 1;

    if (DeviceState_Running == state) {
        state = DeviceState_Armed;
        is_ok = 0;

        try {
            // must precede close of chunk file
            write_group_metadata_();
            metadata_sinks_.clear();

            for (auto& writer : writers_) {
                writer->finalize();
            }

            // call await_stop() before destroying to give jobs a chance to
            // finish
            thread_pool_->await_stop();
            thread_pool_ = nullptr;

            connection_pool_ = nullptr;

            // don't clear before all working threads have shut down
            writers_.clear();

            // should be empty, but just in case
            for (auto& [_, frame] : scaled_frames_) {
                if (frame && *frame) {
                    free(*frame);
                }
            }
            scaled_frames_.clear();

            error_ = false;
            error_msg_.clear();

            is_ok = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }
    }

    return is_ok;
}

size_t
zarr::Zarr::append(const VideoFrame* frames, size_t nbytes)
{
    CHECK(DeviceState_Running == state);
    EXPECT(!error_, "%s", error_msg_.c_str());

    if (0 == nbytes) {
        return nbytes;
    }

    const VideoFrame* cur = nullptr;
    const auto* end = (const VideoFrame*)((uint8_t*)frames + nbytes);
    auto next = [&]() -> const VideoFrame* {
        const uint8_t* p = ((const uint8_t*)cur) + cur->bytes_of_frame;
        return (const VideoFrame*)p;
    };

    for (cur = frames; cur < end; cur = next()) {
        const size_t bytes_of_frame = bytes_of_image(&cur->shape);
        const size_t bytes_written = append_frame(
          const_cast<uint8_t*>(cur->data), bytes_of_frame, cur->shape);
        EXPECT(bytes_written == bytes_of_frame,
               "Expected to write %zu bytes, but wrote %zu.",
               bytes_of_frame,
               bytes_written);
    }

    return nbytes;
}

size_t
zarr::Zarr::append_frame(const uint8_t* const data,
                         size_t bytes_of_data,
                         const ImageShape& shape)
{
    CHECK(DeviceState_Running == state);
    EXPECT(!error_, "%s", error_msg_.c_str());

    if (!data || !bytes_of_data) {
        return 0;
    }

    const size_t bytes_written = writers_.at(0)->write(data, bytes_of_data);
    if (bytes_written != bytes_of_data) {
        set_error("Failed to write frame.");
        return bytes_written;
    }

    // multiscale
    if (writers_.size() > 1) {
        write_multiscale_frames_(data, bytes_written, shape);
    }

    return bytes_written;
}

void
zarr::Zarr::reserve_image_shape(const ImageShape* shape)
{
    EXPECT(state != DeviceState_Running,
           "Cannot reserve image shape while running.");

    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);

    // image shape should be compatible with first two acquisition dimensions
    EXPECT(shape->dims.width == acquisition_dimensions_.at(0).array_size_px,
           "Image width must match first acquisition dimension.");
    EXPECT(shape->dims.height == acquisition_dimensions_.at(1).array_size_px,
           "Image height must match second acquisition dimension.");

    image_shape_ = *shape;
}

/// Zarr

zarr::Zarr::Zarr()
  : Storage {
      .state = DeviceState_AwaitingConfiguration,
      .set = ::zarr_set,
      .get = ::zarr_get,
      .get_meta = ::zarr_get_meta,
      .start = ::zarr_start,
      .append = ::zarr_append,
      .stop = ::zarr_stop,
      .destroy = ::zarr_destroy,
      .reserve_image_shape = ::zarr_reserve_image_shape,
  }
  , thread_pool_{ nullptr }
  , pixel_scale_um_{ 1, 1 }
  , enable_multiscale_{ false }
  , error_{ false }
{
}

zarr::Zarr::Zarr(BloscCompressionParams&& compression_params)
  : Zarr()
{
    blosc_compression_params_ = std::move(compression_params);
}

void
zarr::Zarr::set_dimensions_(const StorageProperties* props)
{
    const auto dimension_count = props->acquisition_dimensions.size;
    EXPECT(dimension_count > 2, "Expected at least 3 dimensions.");

    acquisition_dimensions_.clear();

    for (auto i = 0; i < dimension_count; ++i) {
        CHECK(props->acquisition_dimensions.data[i].name.str);
        Dimension dim(props->acquisition_dimensions.data[i]);
        validate_dimension(dim, i == dimension_count - 1);

        acquisition_dimensions_.push_back(dim);
    }
}

void
zarr::Zarr::set_error(const std::string& msg) noexcept
{
    std::scoped_lock lock(mutex_);

    // don't overwrite the first error
    if (!error_) {
        error_ = true;
        error_msg_ = msg;
    }
}

void
zarr::Zarr::write_fixed_metadata_() const
{
    write_base_metadata_();
    write_external_metadata_();
}

json
zarr::Zarr::make_multiscale_metadata_() const
{
    json multiscales = json::array({ json::object() });
    // write multiscale metadata
    multiscales[0]["version"] = "0.4";

    auto& axes = multiscales[0]["axes"];
    for (auto dim = acquisition_dimensions_.rbegin();
         dim != acquisition_dimensions_.rend();
         ++dim) {
        std::string type;
        switch (dim->kind) {
            case DimensionType_Space:
                type = "space";
                break;
            case DimensionType_Channel:
                type = "channel";
                break;
            case DimensionType_Time:
                type = "time";
                break;
            case DimensionType_Other:
                type = "other";
                break;
            default:
                throw std::runtime_error("Unknown dimension type");
        }

        if (dim < acquisition_dimensions_.rend() - 2) {
            axes.push_back({ { "name", dim->name }, { "type", type } });
        } else {
            axes.push_back({ { "name", dim->name },
                             { "type", type },
                             { "unit", "micrometer" } });
        }
    }

    // spatial multiscale metadata
    if (writers_.empty()) {
        std::vector<double> scales;
        for (auto i = 0; i < acquisition_dimensions_.size() - 2; ++i) {
            scales.push_back(1.);
        }
        scales.push_back(pixel_scale_um_.y);
        scales.push_back(pixel_scale_um_.x);

        multiscales[0]["datasets"] = {
            {
              { "path", "0" },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale", scales },
                  },
                }
              },
            },
        };
    } else {
        for (auto i = 0; i < writers_.size(); ++i) {
            std::vector<double> scales;
            scales.push_back(std::pow(2, i)); // append
            for (auto k = 0; k < acquisition_dimensions_.size() - 3; ++k) {
                scales.push_back(1.);
            }
            scales.push_back(std::pow(2, i) * pixel_scale_um_.y); // y
            scales.push_back(std::pow(2, i) * pixel_scale_um_.x); // x

            multiscales[0]["datasets"].push_back({
              { "path", std::to_string(i) },
              { "coordinateTransformations",
                {
                  {
                    { "type", "scale" },
                    { "scale", scales },
                  },
                }
              },
            });
        }

        // downsampling metadata
        multiscales[0]["type"] = "local_mean";
        multiscales[0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters are "
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
zarr::Zarr::write_multiscale_frames_(const uint8_t* const data_,
                                     size_t bytes_of_data,
                                     const ImageShape& shape_)
{
    auto* data = const_cast<uint8_t*>(data_);
    ImageShape shape = shape_;
    struct VideoFrame* dst;

    std::function<VideoFrame*(const uint8_t*, size_t, const ImageShape&)> scale;
    std::function<void(VideoFrame*, const VideoFrame*)> average2;
    switch (shape.type) {
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            scale = ::scale_image<uint16_t>;
            average2 = ::average_two_frames<uint16_t>;
            break;
        case SampleType_i8:
            scale = ::scale_image<int8_t>;
            average2 = ::average_two_frames<int8_t>;
            break;
        case SampleType_i16:
            scale = ::scale_image<int16_t>;
            average2 = ::average_two_frames<int16_t>;
            break;
        case SampleType_f32:
            scale = ::scale_image<float>;
            average2 = ::average_two_frames<float>;
            break;
        case SampleType_u8:
            scale = ::scale_image<uint8_t>;
            average2 = ::average_two_frames<uint8_t>;
            break;
        default:
            char err_msg[64];
            snprintf(err_msg,
                     sizeof(err_msg),
                     "Unsupported pixel type: %s",
                     common::sample_type_to_string(shape.type));
            throw std::runtime_error(err_msg);
    }

    for (auto i = 1; i < writers_.size(); ++i) {
        dst = scale(data, bytes_of_data, shape);
        if (scaled_frames_.at(i).has_value()) {
            // average
            average2(dst, scaled_frames_.at(i).value());

            // write the downsampled frame
            const size_t bytes_of_frame = bytes_of_image(&dst->shape);
            CHECK(writers_.at(i)->write(dst->data, bytes_of_frame));

            // clean up this level of detail
            free(scaled_frames_.at(i).value());
            scaled_frames_.at(i).reset();

            // setup for next iteration
            if (i + 1 < writers_.size()) {
                data = dst->data;
                shape = dst->shape;
                bytes_of_data = bytes_of_image(&shape);
            } else {
                // no longer needed
                free(dst);
            }
        } else {
            scaled_frames_.at(i) = dst;
            break;
        }
    }
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

///< Test that a single frame with 1 plane is padded and averaged correctly.
template<typename T>
void
test_average_frame_inner(const SampleType& stype)
{
    const size_t bytes_of_frame =
      common::align_up(sizeof(VideoFrame) + 9 * sizeof(T), 8);
    auto* src = (VideoFrame*)malloc(bytes_of_frame);
    CHECK(src);

    src->bytes_of_frame = bytes_of_frame;
    src->shape = {
        .dims = {
          .channels = 1,
          .width = 3,
          .height = 3,
          .planes = 1,
        },
        .strides = {
          .channels = 1,
          .width = 1,
          .height = 3,
          .planes = 9
        },
        .type = stype
    };

    for (auto i = 0; i < 9; ++i) {
        ((T*)src->data)[i] = (T)(i + 1);
    }

    auto dst =
      scale_image<T>(src->data, bytes_of_image(&src->shape), src->shape);
    CHECK(((T*)dst->data)[0] == (T)3);
    CHECK(((T*)dst->data)[1] == (T)4.5);
    CHECK(((T*)dst->data)[2] == (T)7.5);
    CHECK(((T*)dst->data)[3] == (T)9);

    free(src);
    free(dst);
}

extern "C" acquire_export int
unit_test__average_frame()
{
    try {
        test_average_frame_inner<uint8_t>(SampleType_u8);
        test_average_frame_inner<int8_t>(SampleType_i8);
        test_average_frame_inner<uint16_t>(SampleType_u16);
        test_average_frame_inner<int16_t>(SampleType_i16);
        test_average_frame_inner<float>(SampleType_f32);
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
        return 0;
    } catch (...) {
        LOGE("Exception: (unknown)");
        return 0;
    }

    return 1;
}
#endif
