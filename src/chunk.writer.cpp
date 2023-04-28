#include "chunk.writer.hh"

#include <algorithm>
#include <filesystem>
#include <string>
#include <thread>

#include "device/props/components.h"
#include "platform.h"

#include "blosc.h"

/// Check that a==b
/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define ASSERT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
    } while (0)

namespace fs = std::filesystem;
namespace zarr = acquire::sink::zarr;

namespace {
size_t
bytes_of_type(const SampleType& type) noexcept
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

void
chunk_write_thread(acquire::sink::zarr::ChunkWriter* writer)
{
    struct clock throttle = {};
    clock_init(&throttle);

    auto& roi = writer->roi();

    const size_t bytes_per_row = roi.bytes_per_row();
    std::vector<uint8_t> fill(bytes_per_row);
    std::fill(fill.begin(), fill.end(), 0);
    do {
        if (auto frame = writer->pop_frame_and_make_current()) {
            uint8_t* region = nullptr;
            while (!roi.finished()) {
                size_t nbytes = frame->next_contiguous_region(roi, &region);
                if (0 < nbytes) {
                    CHECK(nullptr != region);
                    writer->write(region, region + nbytes);
                }
                if (nbytes < bytes_per_row) {
                    writer->write(fill.data(),
                                  fill.data() + bytes_per_row - nbytes);
                }
            }

            roi.reset();

            writer->release_current_frame();
        } else
            clock_sleep_ms(&throttle, 50.0);
    } while (writer->should_wait_for_work());
}
} // ::{anonymous}

namespace acquire::sink::zarr {
BloscCompressor::BloscCompressor()
  : clevel_{ 1 }
  , shuffle_{ 1 }
  , available_threads_{ (int)std::thread::hardware_concurrency() }
{
}

BloscCompressor::BloscCompressor(const std::string& codec_id,
                                 int clevel,
                                 int shuffle)
  : codec_id_{ codec_id }
  , clevel_{ clevel }
  , shuffle_{ shuffle }
  , available_threads_{ (int)std::thread::hardware_concurrency() }
{
}

std::vector<std::string>
BloscCompressor::supported_codecs()
{
    return {
        BLOSC_BLOSCLZ_COMPNAME, BLOSC_LZ4_COMPNAME,  BLOSC_LZ4HC_COMPNAME,
        BLOSC_SNAPPY_COMPNAME,  BLOSC_ZLIB_COMPNAME, BLOSC_ZSTD_COMPNAME,
    };
}

ChunkWriter::ChunkWriter(const acquire::sink::zarr::FrameROI& roi,
                         size_t bytes_per_chunk,
                         Encoder* encoder)
  : encoder_{ encoder }
  , roi_{ roi }
  , bytes_per_chunk_{ 0 }
  , bytes_written_{ 0 }
  , current_chunk_{ 0 }
  , lock_{}
  , thread_{ nullptr }
  , compressor_{}
  , should_wait_for_work_{ false }
{
    CHECK(encoder_);
    const auto bpt = (float)roi.bytes_per_tile();
    tiles_per_chunk_ = std::floor((float)bytes_per_chunk / bpt);
    EXPECT(tiles_per_chunk_ > 0,
           "Given %lu bytes per chunk, %lu bytes per roi.",
           bytes_per_chunk,
           roi.bytes_per_tile());

    bytes_per_chunk_ = tiles_per_chunk_ * roi.bytes_per_tile();
}

ChunkWriter::~ChunkWriter()
{
    if (thread_) {
        LOGE("WARNING: Manually releasing thread!");
        release_thread();
    }
    close_current_file();
    delete encoder_;
}

void
ChunkWriter::push_frame(const TiledFrame* frame)
{
    lock_acquire(&lock_);
    CHECK(frame);
    frame_ptrs_.emplace(frame);
    frame_ids_.emplace(frame->frame_id());
    lock_release(&lock_);
}

bool
ChunkWriter::has_frame(uint64_t frame_id) const
{
    lock_acquire(&lock_);
    if (current_frame_id_.has_value() && current_frame_id_ == frame_id) {
        lock_release(&lock_);
        return true;
    }

    bool contains = frame_ids_.contains(frame_id);
    lock_release(&lock_);
    return contains;
}

bool
ChunkWriter::has_thread() const
{
    return thread_ != nullptr;
}

size_t
ChunkWriter::active_frames() const
{
    size_t nf = frame_ptrs_.size() + current_frame_id_.has_value();

    return nf;
}

bool
ChunkWriter::should_wait_for_work() const
{
    lock_acquire(&lock_);
    bool res = should_wait_for_work_;
    lock_release(&lock_);

    return res;
}

size_t
ChunkWriter::write(const uint8_t* beg, const uint8_t* end)
{
    size_t bytes_out = 0;
    auto* cur = (uint8_t*)beg;

    // we should never see this, but if the number of bytes brings us past
    // the chunk boundary, we need to rollover
    const size_t bytes_of_this_chunk = bytes_written_ % bytes_per_chunk_;
    if ((end - beg) + bytes_of_this_chunk > bytes_per_chunk_) {
        const size_t bytes_remaining = bytes_per_chunk_ - bytes_of_this_chunk;

        bytes_out = encoder_->write(beg, beg + bytes_remaining);
        bytes_written_ += bytes_out;
        if (bytes_out && bytes_written_ % bytes_per_chunk_ == 0)
            rollover();

        cur += bytes_out;
    }

    if (auto b = encoder_->write(cur, end); b > 0) {
        bytes_written_ += b;
        bytes_out += b;

        if (bytes_written_ % bytes_per_chunk_ == 0)
            rollover();
    }

    return bytes_out;
}

const TiledFrame*
ChunkWriter::pop_frame_and_make_current()
{
    lock_acquire(&lock_);
    if (frame_ptrs_.empty()) {
        current_frame_id_.reset();
        lock_release(&lock_);
        return nullptr;
    }

    const TiledFrame* frame = frame_ptrs_.front();
    current_frame_id_ = frame->frame_id();

    frame_ptrs_.pop();
    roi_.reset();
    lock_release(&lock_);

    return frame;
}

void
ChunkWriter::release_current_frame()
{
    lock_acquire(&lock_);
    if (current_frame_id_.has_value()) {
        frame_ids_.erase(current_frame_id_.value());
        current_frame_id_.reset();
    }
    lock_release(&lock_);
}

void
ChunkWriter::assign_thread(thread_t** t)
{
    EXPECT(!thread_, "Thread already assigned.");
    EXPECT(t, "Null thread passed.");

    thread_ = *t;
    *t = nullptr;
    should_wait_for_work_ = true;
    thread_create(thread_, (void (*)(void*))chunk_write_thread, (void*)this);
}

thread_t*
ChunkWriter::release_thread()
{
    lock_acquire(&lock_);
    should_wait_for_work_ = false;
    lock_release(&lock_);

    if (!thread_)
        return nullptr;

    thread_join(thread_);

    thread_t* tmp = thread_;
    thread_ = nullptr;
    return tmp;
}

FrameROI&
ChunkWriter::roi()
{
    return roi_;
}

size_t
ChunkWriter::bytes_per_tile() const
{
    return roi_.bytes_per_tile();
}

size_t
ChunkWriter::bytes_per_chunk() const
{
    return bytes_per_chunk_;
}

size_t
ChunkWriter::tiles_written() const
{
    return bytes_written() / bytes_per_tile();
}

size_t
ChunkWriter::bytes_written() const
{
    return bytes_written_;
}

void
ChunkWriter::set_base_directory(const std::string& base_directory)
{
    EXPECT(fs::is_directory(base_directory),
           R"(Base directory "%s" does not exist or is not a directory.)",
           base_directory.c_str());
    base_dir_ = base_directory;
    create_new_file();
}

void
ChunkWriter::create_new_file()
{
    lock_acquire(&lock_);
    current_chunk_file_path_ =
      (fs::path(base_dir_) / "0" / std::to_string(current_chunk_) / "0" /
       std::to_string(roi_.y()) / std::to_string(roi_.x()))
        .string();

    fs::path chunk_file_path(current_chunk_file_path_);
    if (!fs::is_directory(chunk_file_path.parent_path()))
        fs::create_directories(chunk_file_path.parent_path());

    encoder_->open_file(current_chunk_file_path_);
    lock_release(&lock_);
}

void
ChunkWriter::close_current_file()
{
    if (nullptr == encoder_)
        return;

    const size_t tiles_out = tiles_written();
    if (tiles_out > tiles_per_chunk_ && tiles_out % tiles_per_chunk_ > 0)
        finalize_chunk();

    encoder_->close_file();
}

void
ChunkWriter::update_current_chunk_file()
{
    close_current_file();
    create_new_file();
}

void
ChunkWriter::finalize_chunk()
{
    size_t bytes_remaining =
      bytes_per_chunk() - (bytes_written_ % bytes_per_chunk_);
    std::vector<uint8_t> zeros(bytes_remaining);
    std::fill(zeros.begin(), zeros.end(), 0);

    bytes_written_ +=
      encoder_->write(zeros.data(), zeros.data() + bytes_remaining);
}

void
ChunkWriter::rollover()
{
    TRACE("Rolling over");

    lock_acquire(&lock_);
    ++current_chunk_;
    lock_release(&lock_);

    update_current_chunk_file();
}
} // namespace acquire::sink::zarr