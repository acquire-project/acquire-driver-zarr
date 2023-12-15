#include "zarr.hh"

#include "writers/zarrv2.writer.hh"
#include "json.hpp"

namespace zarr = acquire::sink::zarr;
using json = nlohmann::json;

namespace {
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

    json::parse(str,
                str + nbytes,
                nullptr, // callback
                true,    // allow exceptions
                true     // ignore comments
    );
}

/// \brief Get the filename from a StorageProperties as fs::path.
/// \param props StorageProperties for the Zarr Storage device.
/// \return fs::path representation of the Zarr data directory.
fs::path
as_path(const StorageProperties& props)
{
    return { props.filename.str,
             props.filename.str + props.filename.nbytes - 1 };
}

/// \brief Check that the StorageProperties are valid.
/// \details Assumes either an empty or valid JSON metadata string and a
/// filename string that points to a writable directory. \param props Storage
/// properties for Zarr. \throw std::runtime_error if the parent of the Zarr
/// data directory is not an existing directory.
void
validate_props(const StorageProperties* props)
{
    EXPECT(props->filename.str, "Filename string is NULL.");
    EXPECT(props->filename.nbytes, "Filename string is zero size.");

    // check that JSON is correct (throw std::exception if not)
    validate_json(props->external_metadata_json.str,
                  props->external_metadata_json.nbytes);

    // check that the filename value points to a writable directory
    {

        auto path = as_path(*props);
        auto parent_path = path.parent_path().string();
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
        state = DeviceState_Running;
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
    DeviceState state;
    try {
        CHECK(self_);
        auto* self = (zarr::Zarr*)self_;
        *nbytes = self->append(frames, *nbytes);
        state = DeviceState_Running;
    } catch (const std::exception& exc) {
        *nbytes = 0;
        LOGE("Exception: %s\n", exc.what());
        state = DeviceState_AwaitingConfiguration;
    } catch (...) {
        *nbytes = 0;
        LOGE("Exception: (unknown)");
        state = DeviceState_AwaitingConfiguration;
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
        CHECK(self->stop());
        state = DeviceState_Armed;
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

void
make_scales(std::vector<std::pair<zarr::ImageDims, zarr::ImageDims>>& shapes)
{
    CHECK(shapes.size() == 1);
    const auto base_image_shape = shapes.at(0).first;
    const auto base_tile_shape = shapes.at(0).second;

    const int downscale = 2;

    uint32_t w = base_image_shape.cols;
    uint32_t h = base_image_shape.rows;

    while (w > base_tile_shape.cols || h > base_tile_shape.rows) {
        w = (w + (w % downscale)) / downscale;
        h = (h + (h % downscale)) / downscale;

        zarr::ImageDims im_shape = base_image_shape;
        im_shape.cols = w;
        im_shape.rows = h;

        zarr::ImageDims tile_shape = base_tile_shape;
        if (tile_shape.cols > w)
            tile_shape.cols = w;

        if (tile_shape.rows > h)
            tile_shape.rows = h;

        shapes.emplace_back(im_shape, tile_shape);
    }
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
        dst->data[i] = (T)(((float)dst->data[i] + (float)src->data[i]) / 2.0f);
    }
}
} // end ::{anonymous} namespace

void
zarr::Zarr::set(const StorageProperties* props)
{
    CHECK(props);

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    // checks the directory exists and is writable
    validate_props(props);
    dataset_root_ = as_path(*props);

    if (props->external_metadata_json.str) {
        external_metadata_json_ = props->external_metadata_json.str;
    }

    pixel_scale_um_ = props->pixel_scale_um;

    // chunking
    image_tile_shapes_.clear();
    image_tile_shapes_.emplace_back();

    set_chunking(props->chunk_dims_px, meta.chunk_dims_px);

    if (props->enable_multiscale && !meta.multiscale.is_supported) {
        // TODO (aliddell): https://github.com/ome/ngff/pull/206
        LOGE("OME-Zarr multiscale not yet supported in Zarr v3. "
             "Multiscale arrays will not be written.");
    }
    enable_multiscale_ =
      meta.multiscale.is_supported && props->enable_multiscale;
}

void
zarr::Zarr::get(StorageProperties* props) const
{
    CHECK(storage_properties_set_filename(props,
                                          dataset_root_.string().c_str(),
                                          dataset_root_.string().size() + 1));
    CHECK(storage_properties_set_external_metadata(
      props,
      external_metadata_json_.c_str(),
      external_metadata_json_.size() + 1));
    props->pixel_scale_um = pixel_scale_um_;

    if (!image_tile_shapes_.empty()) {
        props->chunk_dims_px.width = image_tile_shapes_.at(0).second.cols;
        props->chunk_dims_px.height = image_tile_shapes_.at(0).second.rows;
    }
    props->chunk_dims_px.planes = planes_per_chunk_;

    props->enable_multiscale = enable_multiscale_;
}

void
zarr::Zarr::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);

    *meta = {
        .chunk_dims_px = {
          .is_supported = 1,
          .width = {
            .writable = 1,
            .low = 32.f,
            .high = (float)std::numeric_limits<uint16_t>::max(),
            .type = PropertyType_FixedPrecision
          },
          .height = {
            .writable = 1,
            .low = 32.f,
            .high = (float)std::numeric_limits<uint16_t>::max(),
            .type = PropertyType_FixedPrecision
          },
          .planes = {
            .writable = 1,
            .low = 32.f,
            .high = (float)std::numeric_limits<uint16_t>::max(),
            .type = PropertyType_FixedPrecision
          },
        },
    };
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

    write_base_metadata_();
    write_group_metadata_();
    write_all_array_metadata_();
    write_external_metadata_();

    thread_pool_ = std::make_shared<common::ThreadPool>(
      std::thread::hardware_concurrency(),
      [this](const std::string& err) { this->set_error(err); });

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
            write_all_array_metadata_(); // must precede close of chunk file
            write_group_metadata_();

            for (auto& writer : writers_) {
                writer->finalize();
            }
            writers_.clear();

            // call await_stop() before destroying to give jobs a chance to
            // finish
            thread_pool_->await_stop();
            thread_pool_ = nullptr;

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
    // `shape` should be verified nonnull in storage_reserve_image_shape, but
    // let's check anyway
    CHECK(shape);
    image_tile_shapes_.at(0).first = {
        .cols = shape->dims.width,
        .rows = shape->dims.height,
    };
    pixel_type_ = shape->type;

    ImageDims& image_shape = image_tile_shapes_.at(0).first;
    ImageDims& tile_shape = image_tile_shapes_.at(0).second;

    // ensure that tile dimensions are compatible with the image shape
    {
        StorageProperties props = { 0 };
        get(&props);
        uint32_t tile_width = props.chunk_dims_px.width;
        if (image_shape.cols > 0 &&
            (tile_width == 0 || tile_width > image_shape.cols)) {
            LOGE("%s. Setting width to %u.",
                 tile_width == 0 ? "Tile width not specified"
                                 : "Specified tile width is too large",
                 image_shape.cols);
            tile_width = image_shape.cols;
        }
        tile_shape.cols = tile_width;

        uint32_t tile_height = props.chunk_dims_px.height;
        if (image_shape.rows > 0 &&
            (tile_height == 0 || tile_height > image_shape.rows)) {
            LOGE("%s. Setting height to %u.",
                 tile_height == 0 ? "Tile height not specified"
                                  : "Specified tile height is too large",
                 image_shape.rows);
            tile_height = image_shape.rows;
        }
        tile_shape.rows = tile_height;

        storage_properties_destroy(&props);
    }

    if (enable_multiscale_) {
        make_scales(image_tile_shapes_);
    }

    allocate_writers_();

    // multiscale
    for (auto i = 1; i < writers_.size(); ++i) {
        scaled_frames_.insert_or_assign(i, std::nullopt);
    }
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
  , planes_per_chunk_{ 0 }
  , enable_multiscale_{ false }
  , pixel_type_{ SampleType_u8 }
  , error_{ false }
{
}

zarr::Zarr::Zarr(BloscCompressionParams&& compression_params)
  : Zarr()
{
    blosc_compression_params_ = std::move(compression_params);
}

void
zarr::Zarr::set_chunking(const ChunkingProps& props, const ChunkingMeta& meta)
{
    // image shape is set *after* this is set so we verify it later
    image_tile_shapes_.at(0).second = {
        .cols = std::clamp(
          props.width, (uint32_t)meta.width.low, (uint32_t)meta.width.high),
        .rows = std::clamp(
          props.height, (uint32_t)meta.height.low, (uint32_t)meta.height.high),
    };

    planes_per_chunk_ = std::clamp(
      props.planes, (uint32_t)meta.planes.low, (uint32_t)meta.planes.high);

    CHECK(planes_per_chunk_ > 0);
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
zarr::Zarr::write_all_array_metadata_() const
{
    namespace fs = std::filesystem;

    for (auto i = 0; i < image_tile_shapes_.size(); ++i) {
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

extern "C"
{
    acquire_export int unit_test__average_frame()
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
}
#endif
