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
pad(void* im_, size_t bytes_of_image, const ImageShape& image_shape)
{
    auto* image = (T*)im_;
    const int downscale = 2;

    const auto width = image_shape.dims.width;
    const auto height = image_shape.dims.height;
    const auto planes = image_shape.dims.planes;

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
void // TODO (aliddell): avoid the pad
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

template<typename T>
void
average_tiled_frames(
  void* buf_,
  size_t bytes_of_buf,
  const std::vector<std::shared_ptr<zarr::TiledFrame>>& frames)
{
    CHECK(bytes_of_buf >= frames.front()->bytes_of_image());

    auto* buf = (T*)buf_;
    size_t n_elements = frames.front()->bytes_of_image() / sizeof(T);
    float factor = 1.f / frames.size();

    for (auto& frame : frames) {
        auto* image = (T*)frame->data();
        for (size_t i = 0; i < n_elements; ++i) {
            buf[i] += factor * image[i];
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
    multiscales_ = get_tile_shapes(image_shape, tile_shape);
    for (int16_t i = 1; i < multiscales_.size(); ++i) {
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

    const ImageShape& image_shape = multiscales_.at(layer - 1).image_shape;
    const auto bytes_padded = get_padded_buffer_size_bytes(image_shape);

    std::vector<uint8_t> im(bytes_padded);
    memcpy(im.data(), frame->data(), frame->bytes_of_image());

    switch (image_shape.type) {
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            pad<uint16_t>(im.data(), im.size(), image_shape);
            average_frame<uint16_t>(im.data(), im.size(), image_shape);
            break;
        case SampleType_i8:
            pad<int8_t>(im.data(), im.size(), image_shape);
            average_frame<int8_t>(im.data(), im.size(), image_shape);
            break;
        case SampleType_i16:
            pad<int16_t>(im.data(), im.size(), image_shape);
            average_frame<int16_t>(im.data(), im.size(), image_shape);
            break;
        case SampleType_f32:
            pad<float>(im.data(), im.size(), image_shape);
            average_frame<float>(im.data(), im.size(), image_shape);
            break;
        case SampleType_u8:
        default:
            pad<uint8_t>(im.data(), im.size(), image_shape);
            average_frame<uint8_t>(im.data(), im.size(), image_shape);
            break;
    }

    frame = std::make_shared<TiledFrame>(im.data(),
                                         frame->frame_id(),
                                         layer,
                                         multiscales_.at(layer).image_shape,
                                         multiscales_.at(layer).tile_shape);

    accumulator.push_back(frame);
    if (accumulator.size() == 2) {
        frame = average_two_frames(layer);
        zarr_->push_frame_to_writers(frame);
        if (layer < multiscales_.size() - 1) {
            downsample_and_accumulate(frame, layer + 1);
        }
        accumulator.clear();
    }
}

std::shared_ptr<TiledFrame>
FrameScaler::average_two_frames(int16_t layer)
{
    std::vector<std::shared_ptr<TiledFrame>>& frames = accumulators_.at(layer);
    size_t nbytes_frame = frames.front()->bytes_of_image();
    std::vector<uint8_t> buf(nbytes_frame);

    const auto& base_image_shape = multiscales_.at(0).image_shape;
    switch (base_image_shape.type) {
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            average_tiled_frames<uint16_t>(buf.data(), nbytes_frame, frames);
            break;
        case SampleType_i8:
            average_tiled_frames<int8_t>(buf.data(), nbytes_frame, frames);
            break;
        case SampleType_i16:
            average_tiled_frames<int16_t>(buf.data(), nbytes_frame, frames);
            break;
        case SampleType_f32:
            average_tiled_frames<float>(buf.data(), nbytes_frame, frames);
            break;
        case SampleType_u8:
        default:
            average_tiled_frames<uint8_t>(buf.data(), nbytes_frame, frames);
            break;
    }

    return std::make_shared<zarr::TiledFrame>(
      (uint8_t*)buf.data(),
      frames.front()->frame_id(),
      layer,
      multiscales_.at(layer).image_shape,
      multiscales_.at(layer).tile_shape);
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
    {
        ImageShape shape{ .dims = { .width = 4, .height = 4, .planes = 1 } };
        memcpy(buf.data(), pad_none.data(), 16 * sizeof(T));
        pad<T>(buf.data(), 16 * sizeof(T), shape);
        for (auto i = 0; i < buf.size(); ++i) {
            CHECK(pad_none.at(i) == buf.at(i));
        }
    }

    // even width, odd height, should have a row of zeros at the end of the
    // buffer
    {
        ImageShape shape{ .dims = { .width = 4, .height = 3, .planes = 1 } };
        std::fill(buf.begin(), buf.end(), (T)255);
        memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
        pad<T>(buf.data(), 16 * sizeof(T), shape);
        for (auto i = 0; i < pad_one.size(); ++i) {
            CHECK(pad_one.at(i) == buf.at(i));
        }
        for (auto i = pad_one.size(); i < buf.size(); ++i) {
            CHECK(0 == buf.at(i));
        }
    }

    // odd width, even height, should have zeros at the end of every row
    {
        ImageShape shape{ .dims = { .width = 3, .height = 4, .planes = 1 } };
        std::fill(buf.begin(), buf.end(), (T)255);
        memcpy(buf.data(), pad_one.data(), 12 * sizeof(T));
        pad<T>(buf.data(), 16 * sizeof(T), shape);

        size_t idx = 0;
        for (auto i = 0; i < 4; ++i) {
            for (auto j = 0; j < 3; ++j) {
                const auto k = i * 4 + j;
                CHECK(pad_one[idx++] == buf[k]);
            }
            const auto k = i * 4 + 3;
            CHECK(0 == buf[k]);
        }
    }

    // odd width, odd height, should have zeros at the end of every row and a
    // row of zeros at the end of the buffer
    {
        ImageShape shape{ .dims = { .width = 3, .height = 3, .planes = 1 } };
        std::fill(buf.begin(), buf.end(), (T)255);
        memcpy(buf.data(), pad_both.data(), 9 * sizeof(T));
        pad<T>(buf.data(), 16 * sizeof(T), shape);
        size_t idx = 0;
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
