#include "chunk.writer.hh"

#include <algorithm>
#include <filesystem>
#include <mutex>
#include <string>

#include "common.hh"
#include "device/props/components.h"
#include "platform.h"

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
bytes_per_tile(const ImageShape& image, const zarr::TileShape& tile)
{
    return zarr::common::bytes_of_type(image.type) * image.dims.channels *
           tile.width * tile.height * tile.planes;
}
} // ::{anonymous}

namespace acquire::sink::zarr {
CompressionParams::CompressionParams()
  : clevel_{ 1 }
  , shuffle_{ 1 }
{
}

CompressionParams::CompressionParams(const std::string& codec_id,
                                     int clevel,
                                     int shuffle)
  : codec_id_{ codec_id }
  , clevel_{ clevel }
  , shuffle_{ shuffle }
{
}

ChunkWriter::ChunkWriter(BaseEncoder* encoder,
                         const ImageShape& image_shape,
                         const TileShape& tile_shape,
                         uint32_t lod,
                         uint32_t tile_col,
                         uint32_t tile_row,
                         uint32_t tile_plane,
                         uint64_t max_bytes_per_chunk,
                         char dimension_separator,
                         const std::string& base_directory,
                         const std::string& chunk_dir_prefix)
  : encoder_{ encoder }
  , bytes_per_chunk_{ 0 }
  , tiles_per_chunk_{ 0 }
  , bytes_written_{ 0 }
  , current_chunk_{ 0 }
  , dimension_separator_{ dimension_separator }
  , base_dir_{ base_directory }
  , current_file_{}
  , layer_{ lod }
  , tile_col_{ tile_col }
  , tile_row_{ tile_row }
  , tile_plane_{ tile_plane }
  , image_shape_{ image_shape }
  , tile_shape_{ tile_shape }
  , chunk_dir_prefix_{ chunk_dir_prefix }
{
    CHECK(encoder_);
    const auto bpt = (float)::bytes_per_tile(image_shape_, tile_shape_);
    EXPECT(bpt > 0, "Computed zero bytes per tile.", bpt);

    tiles_per_chunk_ = std::floor((float)max_bytes_per_chunk / bpt);
    EXPECT(tiles_per_chunk_ > 0,
           "Given %lu bytes per chunk, %lu bytes per tile.",
           max_bytes_per_chunk,
           common::bytes_of_type(image_shape.type));

    // this is guaranteed to be positive
    bytes_per_chunk_ = tiles_per_chunk_ * (size_t)bpt;

    EXPECT('.' == dimension_separator || '/' == dimension_separator,
           "Expecting either '.' or '/' for dimension separator, got '%c'.",
           dimension_separator);
}

ChunkWriter::~ChunkWriter()
{
    close_current_file();
    delete encoder_;
}

bool
ChunkWriter::write_frame(const TiledFrame& frame)
{
    std::scoped_lock lock(mutex_);
    const size_t bpt = ::bytes_per_tile(image_shape_, tile_shape_);
    if (buffer_.size() < bpt)
        buffer_.resize(bpt);

    uint8_t* data = buffer_.data();
    size_t nbytes =
      frame.copy_tile(data, bpt, tile_col_, tile_row_, tile_plane_);

    nbytes = write(data, data + nbytes);

    return nbytes == bpt;
}

const ImageShape&
ChunkWriter::image_shape() const noexcept
{
    return image_shape_;
}

const TileShape&
ChunkWriter::tile_shape() const noexcept
{
    return tile_shape_;
}

uint32_t
ChunkWriter::frames_written() const
{
    const uint64_t bpt = bytes_per_tile(image_shape_, tile_shape_);
    CHECK(bpt > 0);
    return (uint32_t)(bytes_written_ / bpt);
}

size_t
ChunkWriter::write(const uint8_t* beg, const uint8_t* end)
{
    const size_t bytes_in = (uint8_t*)end - (uint8_t*)beg;
    if (0 == bytes_in)
        return 0;

    if (!current_file_.has_value())
        open_chunk_file();

    size_t bytes_out = 0;
    auto* cur = (uint8_t*)beg;

    // we should never see this, but if the number of bytes brings us past
    // the chunk boundary, we need to rollover
    CHECK(bytes_per_chunk_ > 0);
    const size_t bytes_of_this_chunk = bytes_written_ % bytes_per_chunk_;
    if (bytes_in + bytes_of_this_chunk > bytes_per_chunk_) {
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

void
ChunkWriter::open_chunk_file()
{
    char file_path[512];
    snprintf(file_path,
             sizeof(file_path) - 1,
             "%d%c%s%d%c%d%c%d%c%d",
             layer_,
             dimension_separator_,
             chunk_dir_prefix_.c_str(),
             current_chunk_,
             dimension_separator_,
             tile_plane_,
             dimension_separator_,
             tile_row_,
             dimension_separator_,
             tile_col_);

    std::string path = (fs::path(base_dir_) / file_path).string();
    auto parent_path = fs::path(path).parent_path();

    if (!fs::is_directory(parent_path))
        fs::create_directories(parent_path);

    current_file_ = file{};
    CHECK(file_create(&current_file_.value(), path.c_str(), path.size()));

    encoder_->set_file(&current_file_.value());
}

void
ChunkWriter::close_current_file()
{
    if (!current_file_.has_value())
        return;

    const size_t bpt = bytes_per_tile(image_shape_, tile_shape_);
    CHECK(bpt > 0);
    const size_t tiles_written = bytes_written_ / bpt;

    if (tiles_written > tiles_per_chunk_ &&
        tiles_written % tiles_per_chunk_ > 0)
        finalize_chunk();

    encoder_->flush();

    file_close(&current_file_.value());
    current_file_.reset();

    encoder_->set_file(nullptr);
}

void
ChunkWriter::finalize_chunk()
{
    CHECK(bytes_per_chunk_ > 0);
    size_t bytes_remaining =
      bytes_per_chunk_ - (bytes_written_ % bytes_per_chunk_);
    std::vector<uint8_t> zeros(bytes_remaining);
    std::fill(zeros.begin(), zeros.end(), 0);

    bytes_written_ +=
      encoder_->write(zeros.data(), zeros.data() + bytes_remaining);
}

void
ChunkWriter::rollover()
{
    TRACE("Rolling over");
    close_current_file();
    ++current_chunk_;
}
} // namespace acquire::sink::zarr