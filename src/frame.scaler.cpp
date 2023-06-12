#include "frame.scaler.hh"

#include <cstring>
#include <thread>

namespace {
static void
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
FrameScaler::FrameScaler(const ImageShape& image_shape,
               const TileShape& tile_shape,
               int16_t max_layer,
               uint8_t downscale)
  : image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
  , max_layer_{ max_layer }
  , downscale_{ downscale }
{
}

FrameScaler::~FrameScaler()
{
    if (!frame_ptrs_.empty())
        LOGE("WARNING: Destroying unwritten frames in Scaler.");

    while (!frame_ptrs_.empty()) {
        delete frame_ptrs_.front();
        frame_ptrs_.pop();
    }
}

void
FrameScaler::push_frame(TiledFrame* frame)
{
    CHECK(frame);
    frame_ptrs_.push(frame);
    frame_ids_.emplace(frame->frame_id());
}

bool
FrameScaler::has_frame(uint64_t frame_id) const
{
    if (current_frame_id_.has_value() && current_frame_id_ == frame_id) {
        return true;
    }

    return frame_ids_.contains(frame_id);
}

size_t
FrameScaler::active_frames() const
{
    size_t nf = frame_ptrs_.size() + current_frame_id_.has_value();

    return nf;
}

const TiledFrame*
FrameScaler::pop_frame_and_make_current()
{
    if (frame_ptrs_.empty()) {
        current_frame_id_.reset();
        return nullptr;
    }

    const TiledFrame* frame = frame_ptrs_.front();
    current_frame_id_ = frame->frame_id();

    frame_ptrs_.pop();

    return frame;
}

void
FrameScaler::release_current_frame()
{
    if (current_frame_id_.has_value()) {
        frame_ids_.erase(current_frame_id_.value());
        current_frame_id_.reset();
    }
};

const ImageShape&
FrameScaler::image_shape() const noexcept
{
    return image_shape_;
}

const TileShape&
FrameScaler::tile_shape() const noexcept
{
    return tile_shape_;
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

std::mutex&
FrameScaler::mutex() noexcept
{
    return mutex_;
}

void
scale_thread(ScalerContext* context)
{
    CHECK(context);

    std::vector<uint8_t> im;

    while (true) {
        std::unique_lock<std::mutex> context_lock(context->mutex);
        context->cv.wait(context_lock, [&]() {
            return context->should_stop || nullptr != context->scaler;
        });

        if (context->should_stop)
            break;

        auto* scaler = context->scaler;
        {
            std::scoped_lock scaler_lock(scaler->mutex());

            if (auto frame = scaler->pop_frame_and_make_current()) {
                if (im.size() < frame->bytes_of_image()) {
                    im.resize(frame->bytes_of_image());
                }

                memcpy(im.data(), frame->data(), frame->bytes_of_image());

                int w = scaler->image_shape().dims.width;
                int h = scaler->image_shape().dims.height;
                int b = scaler->max_layer() == -1 ? std::max(w, h)
                                                  : scaler->max_layer();

                while (b) {
                    bin(im.data(), scaler->downscale(), w, h);
                    b >>= 1;
                    w >>= 1;
                    h >>= 1;

                    ImageShape im_shape = scaler->image_shape();
                    im_shape.dims.width = w;
                    im_shape.dims.height = h;
                    im_shape.strides.width = im_shape.strides.channels * w;
                    im_shape.strides.height = im_shape.strides.width * h;
                    im_shape.strides.planes =
                      im_shape.strides.height * im_shape.dims.planes;

                    TileShape tile_shape = scaler->tile_shape();
                    if (tile_shape.dims.width > w)
                        tile_shape.dims.width = w;

                    if (tile_shape.dims.height > h)
                        tile_shape.dims.height = h;

                    TiledFrame* new_frame = new TiledFrame(
                      im.data(),
                      w * h * bytes_of_type(scaler->image_shape().type),
                      frame->frame_id(),
                      im_shape,
                      tile_shape);

                    context->callback(new_frame);
                }
                scaler->release_current_frame();
            }
        }
    }
}
} // namespace acquire::sink::zarr
