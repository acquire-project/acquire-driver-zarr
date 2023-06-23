#include "frame.scaler.hh"
#include "zarr.hh"

#include <cstring>
#include <thread>

namespace {

void
pad(uint8_t* im_, size_t bytes_of_image, int w, int h)
{
    const int N = 2;
    if (w % N == 0 && h % N == 0)
        return;

    int w_pad = w + (w % N), h_pad = h + (h % N);
    LOG("padding: %d => %d, %d => %d", w, w_pad, h, h_pad);
    size_t bytes_pad = w_pad * h_pad;
    CHECK(bytes_pad <= bytes_of_image);

    auto buf = new uint8_t[bytes_pad];
    memset(buf, 0, bytes_pad);

    size_t offset = 0;
    for (auto i = 0; i < h; ++i) {
        memcpy(buf + offset, im_ + i * w, w);
        offset += w_pad;
    }

    memcpy(im_, buf, bytes_pad);
    delete[] buf;
}

void
bin(uint8_t* const im_, size_t bytes_of_image, int w, int h)
{
    const int N = 2;
    const auto factor = 1.f / (float)N;
    int w_pad = w + (w % N), h_pad = h + (h % N);

    const uint8_t* end = im_ + w_pad * h_pad;

    // horizontal
    for (uint8_t* row = im_; row < end; row += w_pad) {
        const uint8_t* row_end = row + w_pad;
        for (uint8_t* p = row; p < row_end; p += N) {
            // p[0] = (uint8_t)(0.5f * (float)p[0] + 0.5f * (float)p[1]);
            float sum = 0.f;
            for (int i = 0; i < N; ++i) {
                sum += factor * (float)p[i];
            }
            p[0] = (uint8_t)sum;
        }
        for (uint8_t *p = row, *s = row; s < row_end; ++p, s += N) {
            *p = *s;
        }
    }

    // vertical
    for (uint8_t* row = im_ + (N - 1) * w_pad; row < end; row += N * w_pad) {
        const uint8_t* row_end = row + N * w_pad;
        for (uint8_t* p = row; p < row_end; ++p) {
            // p[-w_pad] = (uint8_t)(0.5f * p[-w_pad] + 0.5f * p[0]);
            float sum = 0.f;
            for (int i = 0; i < N; ++i) {
                sum += factor * (float)p[-(w_pad * i)];
            }
        }
    }
    for (uint8_t *src_row = im_, *dst_row = im_; src_row < end;
         src_row += N * w_pad, dst_row += w_pad) {
        memcpy(dst_row, src_row, w_pad);
    }
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

size_t
next_pow2(size_t n)
{
    return n == 0 ? 0 : (size_t)std::pow(2, std::ceil(std::log2((double)n)));
}

size_t
get_padded_buffer_size_bytes(const ImageShape& shape)
{
    auto width = shape.dims.width;
    auto height = shape.dims.width;
    auto planes = shape.dims.planes;

    width += (width % 2);
    height += (height % 2);
    //    planes += (planes % 2);

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

        std::vector<Multiscale> multiscales =
          get_tile_shapes(image_shape_, tile_shape_, max_layer_, downscale_);

        size_t bytes_padded =
          get_padded_buffer_size_bytes(multiscales[0].image);

        std::vector<uint8_t> im(bytes_padded);
        memcpy(im.data(), frame->data(), frame->bytes_of_image());

        for (auto layer = 1; layer < multiscales.size(); ++layer) {
            const ImageShape& image_shape = multiscales[layer].image;
            const TileShape& tile_shape = multiscales[layer].tile;

            pad(im.data(),
                im.size(),
                image_shape.dims.width,
                image_shape.dims.height);
            bin(im.data(),
                im.size(),
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
        if (tile_shape.width > w)
            tile_shape.width = w;

        if (tile_shape.height > h)
            tile_shape.height = h;

        shapes.emplace_back(im_shape, tile_shape);
    }

    return shapes;
}
} // namespace acquire::sink::zarr
