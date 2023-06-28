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

// TODO: template
void
pad(uint8_t* image, size_t bytes_of_image, size_t width, size_t height)
{
    const int downscale = 2;
    if (width % downscale == 0 && height % downscale == 0)
        return;

    size_t w_pad = width + (width % downscale),
           h_pad = height + (height % downscale);
    TRACE("padding: %d => %d, %d => %d", width, w_pad, height, h_pad);
    size_t nbytes_pad = w_pad * h_pad;
    CHECK(nbytes_pad <= bytes_of_image);

    if (height != h_pad) {
        memset(image + (h_pad - 1) * w_pad, 0, w_pad);
    }

    if (width != w_pad) {
        for (auto i = (int)height - 1; i >= 0; --i) {
            memmove(image + i * w_pad, image + i * width, width);
            image[(i + 1) * w_pad - 1] = 0;
        }
    }
}

// TODO: template
void
average2d(uint8_t* image, size_t bytes_of_image, const ImageShape& shape)
{
    const int downscale = 2;
    const auto factor = 1.f / std::pow((float)downscale, 2.f);
    const auto width = shape.dims.width + (shape.dims.width % downscale);
    const auto height = shape.dims.height + (shape.dims.height % downscale);

    if (width < downscale || height < downscale)
        return; // Not enough pixels to form a 2x2 block

    const auto half_width = width / downscale;
    const auto bytes = bytes_of_type(shape.type);

    CHECK(bytes_of_image >= width * height * bytes);
    for (auto i = 0; i < height; i += downscale) {
        for (auto j = 0; j < width; j += downscale) {
            auto k = i * width + j;
            // if downscale were larger than 2, we'd have more summands
            image[k] = (uint8_t)(factor * (float)image[k] +
                                 factor * (float)image[k + 1] +
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
        memcpy(image + offset, image + i * width, half_width);
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
    CHECK(max_layer >= -1);

    std::vector<Multiscale> shapes;
    shapes.emplace_back(base_image_shape, base_tile_shape);
    if (max_layer == 0) {
        return shapes;
    }

    const int downscale = 2;

    uint32_t w = base_image_shape.dims.width;
    uint32_t h = base_image_shape.dims.height;
    uint32_t b = max_layer == -1 ? std::max(w, h) : max_layer;

    for (; b > 0; b /= downscale) {
        if (max_layer == -1 && w <= base_tile_shape.width &&
            h <= base_tile_shape.height) {
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
