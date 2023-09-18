#include "common.hh"

#include "platform.h"

#include <cmath>

namespace common = acquire::sink::zarr::common;

size_t
common::bytes_of_type(const SampleType& type)
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

size_t
common::bytes_per_tile(const ImageDims& tile_shape, const SampleType& type)
{
    return bytes_of_type(type) * tile_shape.rows * tile_shape.cols;
}

size_t
common::frames_per_chunk(const ImageDims& tile_shape,
                         SampleType type,
                         uint64_t max_bytes_per_chunk)
{
    auto bpt = (float)bytes_per_tile(tile_shape, type);
    if (0 == bpt)
        return 0;

    return (size_t)std::floor((float)max_bytes_per_chunk / bpt);
}

size_t
common::bytes_per_chunk(const ImageDims& tile_shape,
                        const SampleType& type,
                        uint64_t max_bytes_per_chunk)
{
    return bytes_per_tile(tile_shape, type) *
           frames_per_chunk(tile_shape, type, max_bytes_per_chunk);
}

const char*
common::sample_type_to_dtype(SampleType t)

{
    static const char* table[] = { "<u1", "<u2", "<i1", "<i2",
                                   "<f4", "<u2", "<u2", "<u2" };
    if (t < countof(table)) {
        return table[t];
    } else {
        throw std::runtime_error("Invalid sample type.");
    }
}

const char*
common::sample_type_to_string(SampleType t) noexcept
{
    static const char* table[] = { "u8",  "u16", "i8",  "i16",
                                   "f32", "u16", "u16", "u16" };
    if (t < countof(table)) {
        return table[t];
    } else {
        return "unrecognized pixel type";
    }
}

void
common::write_string(const std::string& path, const std::string& str)
{
    if (auto p = fs::path(path); !fs::exists(p.parent_path()))
        fs::create_directories(p.parent_path());

    struct file f = { 0 };
    auto is_ok = file_create(&f, path.c_str(), path.size());
    is_ok &= file_write(&f,                                  // file
                        0,                                   // offset
                        (uint8_t*)str.c_str(),               // cur
                        (uint8_t*)(str.c_str() + str.size()) // end
    );
    EXPECT(is_ok, "Write to \"%s\" failed.", path.c_str());
    TRACE("Wrote %d bytes to \"%s\".", str.size(), path.c_str());
    file_close(&f);
}
