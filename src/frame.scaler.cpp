#include "frame.scaler.hh"
#include "zarr.hh"

#include <cstring>
#include <thread>

namespace {
namespace zarr = acquire::sink::zarr;

size_t
bytes_of_type(const SampleType& type)
{
    CHECK(type < SampleTypeCount);
    static size_t table[SampleTypeCount]; // = { 1, 2, 1, 2, 4, 2, 2, 2 };
#define XXX(s, b) table[(s)] = (b)
    XXX(SampleType_u8, 1);
    XXX(SampleType_u16, 2);
    XXX(SampleType_i8, 1);
    XXX(SampleType_i16, 2);
    XXX(SampleType_f32, 4);
    XXX(SampleType_u10, 2);
    XXX(SampleType_u12, 2);
    XXX(SampleType_u14, 2);
#undef XXX
    return table[type];
}

template<typename T>
void
average_one_frame(std::shared_ptr<zarr::TiledFrame> dst,
                  std::shared_ptr<zarr::TiledFrame> src)
{
    CHECK(dst);
    CHECK(src);

    const auto& src_shape = src->image_shape();
    const int downscale = 2;
    const auto factor = 0.125f;

    const auto width = src_shape.dims.width;
    const auto w_pad = width + (width % downscale);

    const auto height = src_shape.dims.height;
    const auto h_pad = height + (height % downscale);

    const auto planes = src_shape.dims.planes;
    const auto p_pad = planes > 1 ? planes + (planes % downscale) : 1;

    CHECK(dst->bytes_of_image() >= w_pad * h_pad * p_pad * factor * sizeof(T));

    const auto* src_img = (T*)src->image();
    auto* dst_img = (T*)dst->data();

    size_t dst_idx = 0;
    for (auto plane = 0; plane < planes; plane += downscale) {
        const bool pad_plane = (plane == planes - 1);

        for (auto row = 0; row < height; row += downscale) {
            const bool pad_height = (row == height - 1 && height != h_pad);

            for (auto col = 0; col < width; col += downscale) {
                const bool pad_width = (col == width - 1 && width != w_pad);

                size_t idx = plane * width * height + row * width + col;
                dst_img[dst_idx++] =
                  (T)(factor *
                      ((float)src_img[idx] +
                       (float)src_img[idx + (1 - (int)pad_width)] +
                       (float)src_img[idx + width * (1 - (int)pad_height)] +
                       (float)src_img[idx + width * (1 - (int)pad_height) +
                                      (1 - (int)pad_width)] +
                       (float)
                         src_img[idx + width * height * (1 - (int)pad_plane)] +
                       (float)
                         src_img[idx + width * height * (1 - (int)pad_plane) +
                                 (1 - (int)pad_width)] +
                       (float)
                         src_img[idx + width * height * (1 - (int)pad_plane) +
                                 width * (1 - (int)pad_height)] +
                       (float)
                         src_img[idx + width * height * (1 - (int)pad_plane) +
                                 width * (1 - (int)pad_height) +
                                 (1 - (int)pad_width)]));
            }
        }
    }
}

template<typename T>
void
average_two_frames(std::shared_ptr<zarr::TiledFrame> dst,
                   std::shared_ptr<zarr::TiledFrame> src1,
                   std::shared_ptr<zarr::TiledFrame> src2)
{
    CHECK(dst);
    CHECK(src1);
    CHECK(src2);

    CHECK(dst->bytes_of_image() == src1->bytes_of_image() &&
          dst->bytes_of_image() == src2->bytes_of_image());

    const float factor = 0.5f;
    const size_t npx = dst->bytes_of_image() / sizeof(T);

    const auto* src1_img = (T*)src1->image();
    const auto* src2_img = (T*)src2->image();
    auto* dst_img = (T*)dst->data();

    for (auto i = 0; i < npx; ++i) {
        dst_img[i] = (T)(factor * ((float)src1_img[i] + (float)src2_img[i]));
    }
}
} // ::<anonymous> namespace

namespace acquire::sink::zarr {
ScalingParameters::ScalingParameters(const ImageShape& image_shape,
                                     const TileShape& tile_shape)
  : image_shape{ image_shape }
  , tile_shape{ tile_shape }
{
}

FrameScaler::FrameScaler(Zarr* zarr,
                         const ImageShape& image_shape,
                         const TileShape& tile_shape)
  : zarr_{ zarr }
{
    CHECK(zarr_);
    scaling_params_ = make_scaling_parameters(image_shape, tile_shape);
    for (int16_t i = 1; i < scaling_params_.size(); ++i) {
        accumulators_.insert({ i, {} });
    }
}

bool
FrameScaler::push_frame(std::shared_ptr<TiledFrame> frame)
{
    std::unique_lock lock(mutex_);
    try {
        zarr_->push_frame_to_writers(frame);
        downsample_and_accumulate(frame, 1);
        return true;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return false;
}

void
FrameScaler::downsample_and_accumulate(std::shared_ptr<TiledFrame> frame,
                                       int16_t layer)
{
    std::vector<std::shared_ptr<TiledFrame>>& accumulator =
      accumulators_.at(layer);

    const ImageShape& image_shape = scaling_params_.at(layer - 1).image_shape;
    auto dst =
      std::make_shared<TiledFrame>(frame->frame_id(),
                                   layer,
                                   scaling_params_.at(layer).image_shape,
                                   scaling_params_.at(layer).tile_shape);

    switch (image_shape.type) {
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            average_one_frame<uint16_t>(dst, frame);
            if (accumulator.size() == 1) {
                auto averaged = std::make_shared<TiledFrame>(dst->frame_id(),
                                                             dst->layer(),
                                                             dst->image_shape(),
                                                             dst->tile_shape());
                average_two_frames<uint16_t>(
                  averaged, accumulator.front(), dst);
                accumulator.clear();

                zarr_->push_frame_to_writers(averaged);
                if (layer < scaling_params_.size() - 1) {
                    downsample_and_accumulate(averaged, layer + 1);
                }
            } else {
                accumulator.push_back(dst);
            }
            break;
        case SampleType_i8:
            average_one_frame<int8_t>(dst, frame);
            if (accumulator.size() == 1) {
                auto averaged = std::make_shared<TiledFrame>(dst->frame_id(),
                                                             dst->layer(),
                                                             dst->image_shape(),
                                                             dst->tile_shape());
                average_two_frames<int8_t>(averaged, accumulator.front(), dst);
                accumulator.clear();

                zarr_->push_frame_to_writers(averaged);
                if (layer < scaling_params_.size() - 1) {
                    downsample_and_accumulate(averaged, layer + 1);
                }
            } else {
                accumulator.push_back(dst);
            }
            break;
        case SampleType_i16:
            average_one_frame<int16_t>(dst, frame);
            if (accumulator.size() == 1) {
                auto averaged = std::make_shared<TiledFrame>(dst->frame_id(),
                                                             dst->layer(),
                                                             dst->image_shape(),
                                                             dst->tile_shape());
                average_two_frames<int16_t>(averaged, accumulator.front(), dst);
                accumulator.clear();

                zarr_->push_frame_to_writers(averaged);
                if (layer < scaling_params_.size() - 1) {
                    downsample_and_accumulate(averaged, layer + 1);
                }
            } else {
                accumulator.push_back(dst);
            }
            break;
        case SampleType_f32:
            average_one_frame<float>(dst, frame);
            if (accumulator.size() == 1) {
                auto averaged = std::make_shared<TiledFrame>(dst->frame_id(),
                                                             dst->layer(),
                                                             dst->image_shape(),
                                                             dst->tile_shape());
                average_two_frames<float>(averaged, accumulator.front(), dst);
                accumulator.clear();

                zarr_->push_frame_to_writers(averaged);
                if (layer < scaling_params_.size() - 1) {
                    downsample_and_accumulate(averaged, layer + 1);
                }
            } else {
                accumulator.push_back(dst);
            }
            break;
        case SampleType_u8:
        default:
            average_one_frame<uint8_t>(dst, frame);
            if (accumulator.size() == 1) {
                auto averaged = std::make_shared<TiledFrame>(dst->frame_id(),
                                                             dst->layer(),
                                                             dst->image_shape(),
                                                             dst->tile_shape());
                average_two_frames<uint8_t>(averaged, accumulator.front(), dst);
                accumulator.clear();

                zarr_->push_frame_to_writers(averaged);
                if (layer < scaling_params_.size() - 1) {
                    downsample_and_accumulate(averaged, layer + 1);
                }
            } else {
                accumulator.push_back(dst);
            }
            break;
    }
}

std::vector<ScalingParameters>
make_scaling_parameters(const ImageShape& base_image_shape,
                        const TileShape& base_tile_shape)
{
    std::vector<ScalingParameters> shapes;
    shapes.emplace_back(base_image_shape, base_tile_shape);

    const int downscale = 2;

    uint32_t w = base_image_shape.dims.width;
    uint32_t h = base_image_shape.dims.height;

    while (w > base_tile_shape.width || h > base_tile_shape.height) {
        w = (w + (w % downscale)) / downscale;
        h = (h + (h % downscale)) / downscale;

        ImageShape im_shape = base_image_shape;
        im_shape.dims.width = w;
        im_shape.dims.height = h;
        im_shape.strides.width = im_shape.strides.channels;
        im_shape.strides.height = im_shape.strides.width * w;
        im_shape.strides.planes = im_shape.strides.height * h;

        TileShape tile_shape = base_tile_shape;
        if (tile_shape.width > w)
            tile_shape.width = w;

        if (tile_shape.height > h)
            tile_shape.height = h;

        shapes.emplace_back(im_shape, tile_shape);
    }

    return shapes;
}
} // namespace acquire::sink::zarr

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
    ImageShape image_shape {
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
    zarr::TileShape tile_shape{ .width = 3, .height = 3, .planes = 1 };

    auto src =
      std::make_shared<zarr::TiledFrame>(0, 0, image_shape, tile_shape);
    for (auto i = 0; i < 9; ++i) {
        ((T*)src->data())[i] = (T)(i + 1);
    }

    image_shape.dims = { .channels = 1, .width = 2, .height = 2, .planes = 1 };
    image_shape.strides = {
        .channels = 1, .width = 1, .height = 2, .planes = 4
    };
    tile_shape = {
        .width = 2,
        .height = 2,
        .planes = 1,
    };

    auto dst =
      std::make_shared<zarr::TiledFrame>(0, 0, image_shape, tile_shape);

    average_one_frame<T>(dst, src);
    CHECK(((T*)dst->image())[0] == (T)3);
    CHECK(((T*)dst->image())[1] == (T)4.5);
    CHECK(((T*)dst->image())[2] == (T)7.5);
    CHECK(((T*)dst->image())[3] == (T)9);
}

///< Test that a single frame with 3 planes is padded and averaged correctly.
template<typename T>
void
test_average_planes_inner(const SampleType& stype)
{
    ImageShape image_shape {
        .dims = {
          .channels = 1,
          .width = 4,
          .height = 4,
          .planes = 3,
        },
        .strides = {
          .channels = 1,
          .width = 1,
          .height = 4,
          .planes = 16
        },
        .type = stype
    };
    zarr::TileShape tile_shape{ .width = 4, .height = 4, .planes = 1 };

    auto src =
      std::make_shared<zarr::TiledFrame>(0, 0, image_shape, tile_shape);
    for (auto i = 0; i < 48; ++i) {
        ((T*)src->data())[i] = (T)(i + 1);
    }

    image_shape.dims = { .channels = 1, .width = 2, .height = 2, .planes = 2 };
    image_shape.strides = {
        .channels = 1, .width = 1, .height = 2, .planes = 4
    };
    tile_shape = {
        .width = 2,
        .height = 2,
        .planes = 2,
    };

    auto dst =
      std::make_shared<zarr::TiledFrame>(0, 0, image_shape, tile_shape);

    average_one_frame<T>(dst, src);
    CHECK(((T*)dst->image())[0] == (T)11.5);
    CHECK(((T*)dst->image())[1] == (T)13.5);
    CHECK(((T*)dst->image())[2] == (T)19.5);
    CHECK(((T*)dst->image())[3] == (T)21.5);
    CHECK(((T*)dst->image())[4] == (T)35.5);
    CHECK(((T*)dst->image())[5] == (T)37.5);
    CHECK(((T*)dst->image())[6] == (T)43.5);
    CHECK(((T*)dst->image())[7] == (T)45.5);
}

extern "C"
{
    acquire_export int unit_test__average_frame()
    {
        try {
            test_average_frame_inner<uint8_t>(SampleType_u8);
            test_average_planes_inner<uint8_t>(SampleType_u8);

            test_average_frame_inner<int8_t>(SampleType_i8);
            test_average_planes_inner<int8_t>(SampleType_i8);

            test_average_frame_inner<uint16_t>(SampleType_u16);
            test_average_planes_inner<uint16_t>(SampleType_u16);

            test_average_frame_inner<int16_t>(SampleType_i16);
            test_average_planes_inner<int16_t>(SampleType_i16);

            test_average_frame_inner<float>(SampleType_f32);
            test_average_planes_inner<float>(SampleType_f32);
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
