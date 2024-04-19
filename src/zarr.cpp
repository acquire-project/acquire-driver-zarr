#include "zarr.hh"

#include "writers/zarrv2.writer.hh"
#include "json.hpp"

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

    const size_t offset =
      strlen(props.uri.str) > 7 && strcmp(props.uri.str, "file://") == 0 ? 7
                                                                         : 0;
    return { props.uri.str + offset,
             props.uri.str + offset + props.uri.nbytes - (offset + 1) };
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
    EXPECT(!uri.starts_with("s3://"), "S3 URIs are not yet supported.");

    // check that the URI value points to a writable directory
    {
        const fs::path path = as_path(*props);
        fs::path parent_path = path.parent_path().string();
        if (parent_path.empty())
            parent_path = ".";

        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.c_str());

        // check directory is writable
        EXPECT(fs::is_directory(parent_path),
               "Expected \"%s\" to be a directory.",
               parent_path.c_str());

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
scale_image(const VideoFrame* src)
{
    CHECK(src);
    const int downscale = 2;
    constexpr size_t bytes_of_type = sizeof(T);
    const auto factor = 0.25f;

    const auto width = src->shape.dims.width;
    const auto w_pad = width + (width % downscale);

    const auto height = src->shape.dims.height;
    const auto h_pad = height + (height % downscale);

    auto* dst = (VideoFrame*)malloc(sizeof(VideoFrame) +
                                    w_pad * h_pad * factor * sizeof(T));
    memcpy(dst, src, sizeof(VideoFrame));

    dst->shape.dims.width = w_pad / downscale;
    dst->shape.dims.height = h_pad / downscale;
    dst->shape.strides.height =
      dst->shape.strides.width * dst->shape.dims.width;
    dst->shape.strides.planes =
      dst->shape.strides.height * dst->shape.dims.height;

    dst->bytes_of_frame =
      dst->shape.dims.planes * dst->shape.strides.planes * sizeof(T) +
      sizeof(*dst);

    const auto* src_img = (T*)src->data;
    auto* dst_img = (T*)dst->data;
    memset(dst_img, 0, dst->bytes_of_frame - sizeof(*dst));

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

    const auto bytes_of_image = dst->bytes_of_frame - sizeof(*dst);
    const auto num_pixels = bytes_of_image / sizeof(T);
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

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    // checks the directory exists and is writable
    validate_props(props);
    // TODO (aliddell): we will eventually support S3 URIs,
    //  dataset_root_ should be a string
    dataset_root_ = as_path(*props);

    if (props->external_metadata_json.str) {
        external_metadata_json_ = props->external_metadata_json.str;
    }

    pixel_scale_um_ = props->pixel_scale_um;

    set_dimensions_(props);

    if (props->enable_multiscale && !meta.multiscale_is_supported) {
        // TODO (aliddell): https://github.com/ome/ngff/pull/206
        LOGE("OME-Zarr multiscale not yet supported in Zarr v3. "
             "Multiscale arrays will not be written.");
    }
    enable_multiscale_ = meta.multiscale_is_supported &&
                         props->enable_multiscale &&
                         is_multiscale_supported(acquisition_dimensions_);
}

void
zarr::Zarr::get(StorageProperties* props) const
{
    CHECK(props);
    storage_properties_destroy(props);

    const std::string dataset_root = dataset_root_.string();
    char* uri = nullptr;
    if (!dataset_root_.empty()) {
        fs::path dataset_root_abs = fs::absolute(dataset_root_);
        CHECK(uri = (char*)malloc(dataset_root_abs.string().size() + 8));
        snprintf(uri,
                 dataset_root_abs.string().size() + 8,
                 "file://%s",
                 dataset_root_abs.string().c_str());
    }

    const size_t bytes_of_filename = uri ? strlen(uri) + 1 : 0;

    const char* metadata = external_metadata_json_.empty()
                             ? nullptr
                             : external_metadata_json_.c_str();
    const size_t bytes_of_metadata =
      metadata ? external_metadata_json_.size() + 1 : 0;

    CHECK(storage_properties_init(props,
                                  0,
                                  uri,
                                  bytes_of_filename,
                                  metadata,
                                  bytes_of_metadata,
                                  pixel_scale_um_,
                                  acquisition_dimensions_.size()));

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

    if (uri) {
        free(uri);
    }
}

void
zarr::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    memset(meta, 0, sizeof(*meta));

    meta->chunking_is_supported = 1;
}

void
zarr::Zarr::start()
{
    error_ = true;

    if (fs::exists(dataset_root_)) {
        std::error_code ec;
        EXPECT(fs::remove_all(dataset_root_, ec),
               R"(Failed to remove folder for "%s": %s)",
               dataset_root_.c_str(),
               ec.message().c_str());
    }
    fs::create_directories(dataset_root_);

    thread_pool_ = std::make_shared<common::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error(err); });

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
            write_mutable_metadata_();
            for (FilesystemSink* sink : metadata_sinks_) {
                sink_close(sink);
            }
            metadata_sinks_.clear();

            for (auto& writer : writers_) {
                writer->finalize();
            }

            // call await_stop() before destroying to give jobs a chance to
            // finish
            thread_pool_->await_stop();
            thread_pool_ = nullptr;

            // don't clear before all working threads have shut down
            writers_.clear();

            // should be empty, but just in case
            for (auto& [_, frame] : scaled_frames_) {
                if (frame.has_value() && frame.value()) {
                    free(frame.value());
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
        EXPECT(writers_.at(0)->write(cur), "%s", error_msg_.c_str());

        // multiscale
        if (writers_.size() > 1) {
            write_multiscale_frames_(cur);
        }
    }
    return nbytes;
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

void
zarr::Zarr::write_mutable_metadata_() const
{
    write_group_metadata_();
    for (auto i = 0; i < writers_.size(); ++i) {
        write_array_metadata_(i);
    }
}

void
zarr::Zarr::write_multiscale_frames_(const VideoFrame* frame)
{
    const VideoFrame* src = frame;
    VideoFrame* dst;

    std::function<VideoFrame*(const VideoFrame*)> scale;
    std::function<void(VideoFrame*, const VideoFrame*)> average2;
    switch (frame->shape.type) {
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
                     common::sample_type_to_string(frame->shape.type));
            throw std::runtime_error(err_msg);
    }

    for (auto i = 1; i < writers_.size(); ++i) {
        dst = scale(src);
        if (scaled_frames_.at(i).has_value()) {
            // average
            average2(dst, scaled_frames_.at(i).value());

            CHECK(writers_.at(i)->write(dst));

            // clean up this level of detail
            free(scaled_frames_.at(i).value());
            scaled_frames_.at(i).reset();

            // setup for next iteration
            if (i + 1 < writers_.size()) {
                src = dst;
            } else {
                free(dst); // FIXME (aliddell): find a way to reuse
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
    auto* src = (VideoFrame*)malloc(sizeof(VideoFrame) + 9 * sizeof(T));
    src->bytes_of_frame = sizeof(*src) + 9 * sizeof(T);
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

    auto dst = scale_image<T>(src);
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
