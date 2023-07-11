#include "frame.scaler.hh"
#include "zarr.hh"

#include <cstring>
#include <thread>

namespace {
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
pad(void* im_,
    size_t bytes_of_image,
    uint32_t width,
    uint32_t height,
    uint32_t planes)
{
    auto* image = (T*)im_;
    const int downscale = 2;

    const auto w_pad = width + (width % downscale),
               h_pad = height + (height % downscale);
    const auto p_pad = (planes > 1 ? planes + (planes % downscale) : 1);

    if (w_pad == width && h_pad == height && p_pad == planes) {
        return;
    }

    TRACE("padding: %d => %d, %d => %d, %d => %d",
          width,
          w_pad,
          height,
          h_pad,
          planes,
          p_pad);
    size_t nbytes_pad = w_pad * h_pad * p_pad * sizeof(T);
    CHECK(nbytes_pad <= bytes_of_image);

    if (height != h_pad) {
        memset(image + (h_pad - 1) * w_pad, 0, w_pad * sizeof(T));
    }

    if (width != w_pad) {
        for (auto i = (int)height - 1; i >= 0; --i) {
            memmove(image + i * w_pad, image + i * width, width * sizeof(T));
            image[(i + 1) * w_pad - 1] = 0;
        }
    }
}

template<typename T>
void
average_frame(void* image_, size_t bytes_of_image, const ImageShape& shape)
{
    const int downscale = 2;
    const auto width = shape.dims.width + (shape.dims.width % downscale);
    const auto height = shape.dims.height + (shape.dims.height % downscale);
    const auto planes = shape.dims.planes > 1
                          ? shape.dims.planes + (shape.dims.planes % downscale)
                          : 1;

    const auto exponent = 2 + (planes > 1 ? 1 : 0); // 2x2x2 or 2x2
    const auto factor = 1.f / std::pow((float)downscale, (float)exponent);

    if (width < downscale || height < downscale)
        return; // Not enough pixels to form a 2x2 block

    const auto half_width = width / downscale;

    CHECK(bytes_of_image >= width * height * sizeof(T));

    auto* image = (T*)image_;
    for (auto plane = 0; plane < planes; plane += downscale) {
        for (auto row = 0; row < height; row += downscale) {
            for (auto col = 0; col < width; col += downscale) {
                auto idx = plane * width * height + row * width + col;
                float next_plane =
                  planes > 1 ? (float)image[idx + width * height] +
                                 (float)image[idx + width * height + 1] +
                                 (float)image[idx + width * height + width] +
                                 (float)image[idx + width * height + width + 1]
                             : 0.f;
                image[idx] =
                  (T)(factor * ((float)image[idx] + (float)image[idx + 1] +
                                (float)image[idx + width] +
                                (float)image[idx + width + 1] + next_plane));
            }

            for (auto j = 1; j < half_width; ++j) {
                auto m = row * width + j, n = row * width + downscale * j;
                image[m] = image[n];
            }
        }
    }

    size_t offset = half_width;
    for (auto plane = 0; plane < planes; ++plane) {
        for (auto row = downscale; row < height; row += downscale) {
            memcpy(image + offset,
                   image + row * width + plane * width * height,
                   half_width * sizeof(T));
            offset += half_width;
        }
    }
}

size_t
get_padded_buffer_size_bytes(const ImageShape& shape)
{
    const int downscale = 2;
    const auto width = shape.dims.width + (shape.dims.width % downscale);
    const auto height = shape.dims.height + (shape.dims.height % downscale);
    auto planes = shape.dims.planes;

    return width * height * planes * bytes_of_type(shape.type);
}
} // ::<anonymous> namespace

namespace acquire::sink::zarr {
Multiscale::Multiscale(const ImageShape& image_shape,
                       const TileShape& tile_shape)
  : image{ image_shape }
  , tile{ tile_shape }
{
}

FrameScaler::FrameScaler(Zarr* zarr,
                         const ImageShape& image_shape,
                         const TileShape& tile_shape)
  : zarr_{ zarr }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
{
    CHECK(zarr_);
}

bool
FrameScaler::scale_frame(std::shared_ptr<TiledFrame> frame) const
{
    try {
        zarr_->push_frame_to_writers(frame);

        std::vector<Multiscale> multiscales =
          get_tile_shapes(image_shape_, tile_shape_);

        size_t bytes_padded =
          get_padded_buffer_size_bytes(multiscales[0].image);

        std::vector<uint8_t> im(bytes_padded);
        memcpy(im.data(), frame->data(), frame->bytes_of_image());

        for (auto layer = 1; layer < multiscales.size(); ++layer) {
            ImageShape& image_shape = multiscales[layer - 1].image;

            switch (image_shape_.type) {
                case SampleType_u10:
                case SampleType_u12:
                case SampleType_u14:
                case SampleType_u16:
                    pad<uint16_t>(im.data(),
                                  im.size(),
                                  image_shape.dims.width,
                                  image_shape.dims.height,
                                  image_shape.dims.planes);
                    average_frame<uint16_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_i8:
                    pad<int8_t>(im.data(),
                                im.size(),
                                image_shape.dims.width,
                                image_shape.dims.height,
                                image_shape.dims.planes);
                    average_frame<int8_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_i16:
                    pad<int16_t>(im.data(),
                                 im.size(),
                                 image_shape.dims.width,
                                 image_shape.dims.height,
                                 image_shape.dims.planes);
                    average_frame<int16_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_f32:
                    pad<float>(im.data(),
                               im.size(),
                               image_shape.dims.width,
                               image_shape.dims.height,
                               image_shape.dims.planes);
                    average_frame<float>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_u8:
                default:
                    pad<uint8_t>(im.data(),
                                 im.size(),
                                 image_shape.dims.width,
                                 image_shape.dims.height,
                                 image_shape.dims.planes);
                    average_frame<uint8_t>(im.data(), im.size(), image_shape);
                    break;
            }

            image_shape = multiscales[layer].image;
            const auto& tile_shape = multiscales[layer].tile;
            auto scale_layer = std::make_shared<TiledFrame>(
              im.data(), frame->frame_id(), layer, image_shape, tile_shape);

            zarr_->push_frame_to_writers(scale_layer);
        }

        return true;
    } catch (const std::exception& exc) {
        LOGE("Exception: %s\n", exc.what());
    } catch (...) {
        LOGE("Exception: (unknown)");
    }

    return false;
}

std::vector<Multiscale>
get_tile_shapes(const ImageShape& base_image_shape,
                const TileShape& base_tile_shape)
{
    std::vector<Multiscale> shapes;
    shapes.emplace_back(base_image_shape, base_tile_shape);

    const int downscale = 2;

    uint32_t w = base_image_shape.dims.width;
    uint32_t h = base_image_shape.dims.height;
    uint32_t b = std::max(w, h);

    for (; b > 0; b /= downscale) {
        if (w <= base_tile_shape.width && h <= base_tile_shape.height) {
            break;
        }
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

template<typename T>
void
test_padding_inner()
{
    // 4 x 4
    const std::vector<T> pad_none(
      { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });

    // 4 x 3 or 3 x 4
    const std::vector<T> pad_one({ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });

    // 3 x 3
    const std::vector<T> pad_both({ 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    std::vector<T> buf(16);

    // both dims even, should do nothing
    memcpy(buf.data(), pad_none.data(), 16 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 4, 4, 1);
    for (auto i = 0; i < buf.size(); ++i) {
        CHECK(pad_none.at(i) == buf.at(i));
    }

    // even width, odd height, should have a row of zeros at the end of the
    // buffer
    std::fill(buf.begin(), buf.end(), (T)255);
    memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 4, 3, 1);
    for (auto i = 0; i < pad_one.size(); ++i) {
        CHECK(pad_one.at(i) == buf.at(i));
    }
    for (auto i = pad_one.size(); i < buf.size(); ++i) {
        CHECK(0 == buf.at(i));
    }

    // odd width, even height, should have zeros at the end of every row
    std::fill(buf.begin(), buf.end(), (T)255);
    memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 3, 4, 1);

    size_t idx = 0;
    for (auto i = 0; i < 4; ++i) {
        for (auto j = 0; j < 3; ++j) {
            const auto k = i * 4 + j;
            CHECK(pad_one[idx++] == buf[k]);
        }
        const auto k = i * 4 + 3;
        CHECK(0 == buf[k]);
    }

    // odd width, odd height, should have zeros at the end of every row and a
    // row of zeros at the end of the buffer
    std::fill(buf.begin(), buf.end(), (T)255);
    memcpy(buf.data(), pad_both.data(), 9 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 3, 3, 1);
    idx = 0;
    for (auto i = 0; i < 3; ++i) {
        for (auto j = 0; j < 3; ++j) {
            const auto k = i * 4 + j;
            CHECK(pad_both[idx++] == buf[k]);
        }
        const auto k = i * 4 + 3;
        CHECK(0 == buf[k]);
    }
    for (auto i = 12; i < buf.size(); ++i) {
        CHECK(0 == buf.at(i));
    }

    LOG("OK (done)");
}

template<typename T>
void
test_average_plane_inner(const SampleType& stype)
{
    std::vector<T> buf({
      1,  2,  3,  4,  // 1st row, 1st plane
      5,  6,  7,  8,  // 2nd row, 1st plane
      9,  10, 11, 12, // 3rd row, 1st plane
      13, 14, 15, 16, // 4th row, 1st plane
      17, 18, 19, 20, // 1st row, 2nd plane
      21, 22, 23, 24, // 2nd row, 2nd plane
      25, 26, 27, 28, // 3rd row, 2nd plane
      29, 30, 31, 32, // 4th row, 2nd plane
    });
    std::vector<T> averaged({
      (T)11.5, // mean([ 1, 2, 5, 6, 17, 18, 21, 22 ])
      (T)13.5, // mean([ 3, 4, 7, 8, 19, 20, 23, 24 ])
      (T)19.5, // mean([ 9, 10, 13, 14, 25, 26, 29, 30 ])
      (T)21.5, // mean([ 11, 12, 15, 16, 27, 28, 31, 32 ])
    });

    ImageShape shape{
        .dims = {
          .channels = 1,
          .width = 4,
          .height = 4,
          .planes = 2,
        },
        .strides = {
          .channels = 1,
          .width = 1,
          .height = 4,
          .planes = 16
        },
        .type = stype,
    };

    average_frame<T>(buf.data(), 32 * sizeof(T), shape);

    for (auto i = 0; i < averaged.size(); ++i) {
        CHECK(averaged.at(i) == buf.at(i));
    }
}

extern "C"
{
    acquire_export int unit_test__padding()
    {
        try {
            test_padding_inner<uint8_t>();
            test_padding_inner<int8_t>();
            test_padding_inner<uint16_t>();
            test_padding_inner<int16_t>();
            test_padding_inner<float>();
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
            return 0;
        } catch (...) {
            LOGE("Exception: (unknown)");
            return 0;
        }

        return 1;
    }

    acquire_export int unit_test__average_plane()
    {
        try {
            test_average_plane_inner<uint8_t>(SampleType_u8);
            test_average_plane_inner<int8_t>(SampleType_i8);
            test_average_plane_inner<uint16_t>(SampleType_u16);
            test_average_plane_inner<int16_t>(SampleType_i16);
            test_average_plane_inner<float>(SampleType_f32);
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
