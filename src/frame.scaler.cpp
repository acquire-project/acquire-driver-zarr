#include "frame.scaler.hh"
#include "zarr.hh"

#include <cstring>
#include <thread>

namespace {
void
bin(uint8_t* im_, int N, int w, int h) // FIXME (aliddell): make N meaningful
{
    const uint8_t* end = im_ + w * h;

    // horizontal
    for (uint8_t* row = im_; row < end; row += w) {
        const uint8_t* row_end = im_ + w;
        for (uint8_t* p = row; p < row_end; p += 2) {
            p[0] = (uint8_t)(0.5f * p[0] + 0.5f * p[1]);
        }
        for (uint8_t *p = row, *s = row; s < row_end; ++p, s += 2) {
            *p = *s;
        }
    }

    // vertical
    for (uint8_t* row = im_ + w; row < end; row += 2 * w) {
        const uint8_t* row_end = im_ + 2 * w;
        for (uint8_t* p = row; p < row_end; ++p) {
            p[-w] = (uint8_t)(0.5f * p[-w] + 0.5f * p[0]);
        }
    }
    for (uint8_t *src_row = im_, *dst_row = im_; src_row < end;
         src_row += 2 * w, dst_row += w) {
        memcpy(dst_row, src_row, w);
    }
}

void
pad(uint8_t* im_, int N, int w, int h) // FIXME (aliddell): make N meaningful
{
    if (w % N == 0 && h % N == 0)
        return;


}

size_t
bytes_of_type(const enum SampleType type)
{
    if (type >= SampleTypeCount)
        return 0;

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
} // ::<anonymous> namespace

namespace acquire::sink::zarr {
FrameScaler::FrameScaler(Zarr* zarr,
                         const ImageShape& image_shape,
                         const TileShape& tile_shape,
                         int16_t max_layer,
                         uint8_t downscale)
  : zarr_{ zarr }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
  , max_layer_{ max_layer }
  , downscale_{ downscale }
{
    CHECK(zarr);
}

int16_t
FrameScaler::max_layer() const noexcept
{
    return max_layer_;
}

uint8_t
FrameScaler::downscale() const noexcept
{
    return downscale_;
}

bool
FrameScaler::scale_frame(std::shared_ptr<TiledFrame> frame) const
{
    try {
        zarr_->push_frame_to_writers(frame);

        std::vector<uint8_t> im(frame->bytes_of_image());
        memcpy(im.data(), frame->data(), frame->bytes_of_image());

        std::vector<Multiscale> multiscales =
          get_tile_shapes(image_shape_, tile_shape_, max_layer_, downscale_);

        for (auto layer = 1; layer < multiscales.size(); ++layer) {
            const ImageShape& image_shape = multiscales[layer].image;
            const TileShape& tile_shape = multiscales[layer].tile;

            bin(im.data(),
                downscale_,
                image_shape.dims.width,
                image_shape.dims.height);

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
                int16_t max_layer,
                uint8_t downscale)
{
    CHECK(downscale > 0);

    std::vector<Multiscale> shapes;
    shapes.emplace_back(base_image_shape, base_tile_shape);

    int w = base_image_shape.dims.width;
    int h = base_image_shape.dims.height;
    int b = max_layer == -1 ? std::max(w, h) : max_layer;

    while (b) {
        b /= downscale;
        w /= downscale;
        h /= downscale;

        if (w == 0 || h == 0) {
            break;
        }

        ImageShape im_shape = base_image_shape;
        im_shape.dims.width = w;
        im_shape.dims.height = h;
        im_shape.strides.width = im_shape.strides.channels;
        im_shape.strides.height = im_shape.strides.width * h;
        im_shape.strides.planes = im_shape.strides.height * w;

        TileShape tile_shape = base_tile_shape;
        if (tile_shape.dims.width > w)
            tile_shape.dims.width = w;

        if (tile_shape.dims.height > h)
            tile_shape.dims.height = h;

        shapes.emplace_back(im_shape, tile_shape);
    }

    return shapes;
}
} // namespace acquire::sink::zarr
