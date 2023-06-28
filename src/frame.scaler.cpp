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

void
pad(uint8_t* image, size_t bytes_of_image, size_t width, size_t height)
{
    const int N = 2;
    if (width % N == 0 && height % N == 0)
        return;

    size_t w_pad = width + (width % N), h_pad = height + (height % N);
    LOG("padding: %d => %d, %d => %d", width, w_pad, height, h_pad);
    size_t nbytes_pad = w_pad * h_pad;
    CHECK(nbytes_pad <= bytes_of_image);

    // TODO (aliddell): avoid the additional alloc here
    std::vector<uint8_t> buf(nbytes_pad, 0);

    for (auto i = 0; i < height; ++i) {
        memcpy(buf.data() + (i * w_pad), image + i * width, width);
    }

    //    std::vector<uint8_t> image_buf(width * height);
    //    memcpy(image_buf.data(), image, width * height);

    memcpy(image, buf.data(), buf.size());
}

void
average2d(uint8_t* image,
          size_t bytes_of_image,
          const ImageShape& shape)
{
    const auto width = shape.dims.width + (shape.dims.width % 2);
    const auto height = shape.dims.height + (shape.dims.height % 2);

    if (width < 2 || height < 2)
        return; // Not enough pixels to form a 2x2 block

    const auto half_width = width / 2;
    const auto bytes = bytes_of_type(shape.type);

    CHECK(bytes_of_image >= width * height * bytes);
    for (auto i = 0; i < height; i += 2) {
        for (auto j = 0; j < width; j += 2) {
            auto k = i * width + j;
            //            auto a_ = k, b_ = k + 1, c_ = k + width, d_ = k +
            //            width + 1; auto a = (float)image[a_], b =
            //            (float)image[b_],
            //                 c = (float)image[c_], d = (float)image[d_];
            image[k] =
              (uint8_t)(0.25f * (float)image[k] + 0.25f * (float)image[k + 1] +
                        0.25f * (float)image[k + width] +
                        0.25f * (float)image[k + width + 1]);
            //            image[k] = (uint8_t)(0.25f * a + 0.25f * b + 0.25f * c
            //            + 0.25f * d);
        }

        for (auto j = 1; j < half_width; ++j) {
            auto m = i * width + j, n = i * width + 2 * j;
            image[m] = image[n];
        }
    }

    size_t offset = half_width;
    for (auto i = 2; i < height; i += 2) {
        memcpy(image + offset, image + i * width, half_width);
        offset += half_width;
    }
}

size_t
next_pow2(size_t n)
{
    return n == 0 ? 0 : (size_t)std::pow(2, std::ceil(std::log2((double)n)));
}

size_t
get_padded_buffer_size_bytes(const ImageShape& shape)
{
    const uint8_t downscale = 2;
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
                         const TileShape& tile_shape,
                         int16_t max_layer)
  : zarr_{ zarr }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
  , max_layer_{ max_layer }
{
    CHECK(zarr);
}

int16_t
FrameScaler::max_layer() const noexcept
{
    return max_layer_;
}

bool
FrameScaler::scale_frame(std::shared_ptr<TiledFrame> frame) const
{
    try {
        zarr_->push_frame_to_writers(frame);

        std::vector<Multiscale> multiscales =
          get_tile_shapes(image_shape_, tile_shape_, max_layer_);

        size_t bytes_padded =
          get_padded_buffer_size_bytes(multiscales[0].image);

        std::vector<uint8_t> im(bytes_padded);
        memcpy(im.data(), frame->data(), frame->bytes_of_image());

        for (auto layer = 1; layer < multiscales.size(); ++layer) {
            ImageShape& image_shape = multiscales[layer - 1].image;

            pad(im.data(),
                im.size(),
                image_shape.dims.width,
                image_shape.dims.height);
            average2d(im.data(), im.size(), image_shape);

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
                const TileShape& base_tile_shape,
                int16_t max_layer)
{
    const uint8_t downscale = 2;

    std::vector<Multiscale> shapes;
    shapes.emplace_back(base_image_shape, base_tile_shape);

    int w = base_image_shape.dims.width;
    int h = base_image_shape.dims.height;
    int b = max_layer == -1 ? std::max(w, h) : max_layer;

    while (b) {
        b /= downscale;
        w = (w + (w % downscale)) / downscale;
        h = (h + (h % downscale)) / downscale;

        if (w == 0 || h == 0) {
            break;
        }

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
