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
pad(void* im_, size_t bytes_of_image, size_t width, size_t height)
{
    auto* image = (T*)im_;
    const int downscale = 2;
    if (width % downscale == 0 && height % downscale == 0)
        return;

    size_t w_pad = width + (width % downscale),
           h_pad = height + (height % downscale);
    TRACE("padding: %d => %d, %d => %d", width, w_pad, height, h_pad);
    size_t nbytes_pad = w_pad * h_pad;
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
average2d(void* im_, size_t bytes_of_image, const ImageShape& shape)
{
    auto* image = (T*)im_;
    const int downscale = 2;
    const auto factor = 1.f / std::pow((float)downscale, 2.f);
    const auto width = shape.dims.width + (shape.dims.width % downscale);
    const auto height = shape.dims.height + (shape.dims.height % downscale);

    if (width < downscale || height < downscale)
        return; // Not enough pixels to form a 2x2 block

    const auto half_width = width / downscale;

    CHECK(bytes_of_image >= width * height * sizeof(T));
    for (auto i = 0; i < height; i += downscale) {
        for (auto j = 0; j < width; j += downscale) {
            auto k = i * width + j;
            // if downscale were larger than 2, we'd have more summands
            image[k] =
              (T)(factor * (float)image[k] + factor * (float)image[k + 1] +
                  factor * (float)image[k + width] +
                  factor * (float)image[k + width + 1]);
        }

        for (auto j = 1; j < half_width; ++j) {
            auto m = i * width + j, n = i * width + downscale * j;
            image[m] = image[n];
        }
    }

    size_t offset = half_width;
    for (auto i = downscale; i < height; i += downscale) {
        memcpy(image + offset, image + i * width, half_width * sizeof(T));
        offset += half_width;
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
    CHECK(zarr);
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
                                  image_shape.dims.height);
                    average2d<uint16_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_i8:
                    pad<int8_t>(im.data(),
                                im.size(),
                                image_shape.dims.width,
                                image_shape.dims.height);
                    average2d<int8_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_i16:
                    pad<int16_t>(im.data(),
                                 im.size(),
                                 image_shape.dims.width,
                                 image_shape.dims.height);
                    average2d<int16_t>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_f32:
                    pad<float>(im.data(),
                               im.size(),
                               image_shape.dims.width,
                               image_shape.dims.height);
                    average2d<float>(im.data(), im.size(), image_shape);
                    break;
                case SampleType_u8:
                default:
                    pad<uint8_t>(im.data(),
                                 im.size(),
                                 image_shape.dims.width,
                                 image_shape.dims.height);
                    average2d<uint8_t>(im.data(), im.size(), image_shape);
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
    pad<T>(buf.data(), 16 * sizeof(T), 4, 4);
    for (auto i = 0; i < buf.size(); ++i) {
        CHECK(pad_none.at(i) == buf.at(i));
    }

    // even width, odd height, should have a row of zeros at the end of the
    // buffer
    std::fill(buf.begin(), buf.end(), 255);
    memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 4, 3);
    for (auto i = 0; i < pad_one.size(); ++i) {
        CHECK(pad_one.at(i) == buf.at(i));
    }
    for (auto i = pad_one.size(); i < buf.size(); ++i) {
        CHECK(0 == buf.at(i));
    }

    // odd width, even height, should have zeros at the end of every row
    std::fill(buf.begin(), buf.end(), 255);
    memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 3, 4);

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
    std::fill(buf.begin(), buf.end(), 255);
    memcpy(buf.data(), pad_both.data(), 9 * sizeof(T));
    pad<T>(buf.data(), 16 * sizeof(T), 3, 3);
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
test_average2d_inner(const SampleType& stype)
{
    std::vector<T> buf({
      1,
      2,
      3,
      4, // 1st row
      5,
      6,
      7,
      8, // 2nd row
      9,
      10,
      11,
      12, // 3rd row
      13,
      14,
      15,
      16, // 4th row
    });
    std::vector<T> averaged({
      (T)3.5,  // avg 1, 2, 5, 6
      (T)5.5,  // avg 3, 4, 7, 8
      (T)11.5, // avg 9, 10, 13, 14
      (T)13.5, // avg 11, 12, 15, 16
    });

    ImageShape shape{
        .dims = {
          .channels = 1,
          .width = 4,
          .height = 4,
          .planes = 1,
        },
        .strides = {
          .channels = 1,
          .width = 1,
          .height = 4,
          .planes = 16
        },
        .type = stype,
    };

    average2d<T>(buf.data(), 16 * sizeof(T), shape);

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

    acquire_export int unit_test__average2d()
    {
        try {
            test_average2d_inner<uint8_t>(SampleType_u8);
            test_average2d_inner<int8_t>(SampleType_i8);
            test_average2d_inner<uint16_t>(SampleType_u16);
            test_average2d_inner<int16_t>(SampleType_i16);
            test_average2d_inner<float>(SampleType_f32);
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
