#include "utilities.hh"
#include "zarr.hh"

#include "platform.h"
#include "thread.pool.hh"

#include <cmath>
#include <thread>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

size_t
common::chunks_along_dimension(const Dimension& dimension)
{
    EXPECT(dimension.chunk_size_px > 0, "Invalid chunk_size size.");

    return (dimension.array_size_px + dimension.chunk_size_px - 1) /
           dimension.chunk_size_px;
}

size_t
common::shards_along_dimension(const Dimension& dimension)
{
    const size_t shard_size = dimension.shard_size_chunks;
    if (shard_size == 0) {
        return 0;
    }

    const size_t n_chunks = chunks_along_dimension(dimension);
    return (n_chunks + shard_size - 1) / shard_size;
}

size_t
common::number_of_chunks_in_memory(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        n_chunks *= chunks_along_dimension(dimensions[i]);
    }

    return n_chunks;
}

size_t
common::number_of_shards(const std::vector<Dimension>& dimensions)
{
    size_t n_shards = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        n_shards *= shards_along_dimension(dim);
    }

    return n_shards;
}

size_t
common::chunks_per_shard(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (const auto& dim : dimensions) {
        n_chunks *= dim.shard_size_chunks;
    }

    return n_chunks;
}

size_t
common::shard_index_for_chunk(size_t chunk_index,
                              const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_strides.push_back(chunk_strides.back() *
                                zarr::common::chunks_along_dimension(dim));
        CHECK(chunk_strides.back());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        chunk_lattice_indices.push_back(chunk_index % chunk_strides.at(i + 1) /
                                        chunk_strides.at(i));
    }
    chunk_lattice_indices.push_back(chunk_index / chunk_strides.back());

    // make shard strides
    std::vector<size_t> shard_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        shard_strides.push_back(shard_strides.back() *
                                zarr::common::shards_along_dimension(dim));
    }

    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices.at(i) /
                                        dimensions.at(i).shard_size_chunks);
    }

    size_t index = 0;
    for (auto i = 0; i < dimensions.size(); ++i) {
        index += shard_lattice_indices.at(i) * shard_strides.at(i);
    }

    return index;
}

size_t
common::shard_internal_index(size_t chunk_idx,
                             const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_strides.push_back(chunk_strides.back() *
                                zarr::common::chunks_along_dimension(dim));
        CHECK(chunk_strides.back());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        chunk_lattice_indices.push_back(chunk_idx % chunk_strides.at(i + 1) /
                                        chunk_strides.at(i));
    }
    chunk_lattice_indices.push_back(chunk_idx / chunk_strides.back());

    // make shard lattice indices
    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices.at(i) /
                                        dimensions.at(i).shard_size_chunks);
    }

    std::vector<size_t> chunk_internal_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_internal_strides.push_back(chunk_internal_strides.back() *
                                         dim.shard_size_chunks);
    }

    size_t index = 0;

    for (auto i = 0; i < dimensions.size(); ++i) {
        index +=
          (chunk_lattice_indices.at(i) % dimensions.at(i).shard_size_chunks) *
          chunk_internal_strides.at(i);
    }

    return index;
}

size_t
common::bytes_per_chunk(const std::vector<Dimension>& dimensions,
                        const SampleType& type)
{
    auto n_bytes = bytes_of_type(type);
    for (const auto& d : dimensions) {
        n_bytes *= d.chunk_size_px;
    }

    return n_bytes;
}

const char*
common::sample_type_to_string(SampleType t) noexcept
{
    switch (t) {
        case SampleType_u8:
            return "u8";
        case SampleType_u16:
            return "u16";
        case SampleType_i8:
            return "i8";
        case SampleType_i16:
            return "i16";
        case SampleType_f32:
            return "f32";
        default:
            return "unrecognized pixel type";
    }
}

size_t
common::align_up(size_t n, size_t align)
{
    EXPECT(align > 0, "Alignment must be greater than zero.");
    return align * ((n + align - 1) / align);
}

std::vector<std::string>
common::split_uri(const std::string& uri)
{
    const char delim = '/';

    std::vector<std::string> out;
    size_t begin = 0, end = uri.find_first_of(delim);

    while (end != std::string::npos) {
        std::string part = uri.substr(begin, end - begin);
        if (!part.empty())
            out.push_back(part);

        begin = end + 1;
        end = uri.find_first_of(delim, begin);
    }

    // Add the last segment of the URI (if any) after the last '/'
    std::string last_part = uri.substr(begin);
    if (!last_part.empty()) {
        out.push_back(last_part);
    }

    return out;
}

void
common::parse_path_from_uri(std::string_view uri,
                            std::string& bucket_name,
                            std::string& path)
{
    auto parts = split_uri(uri.data());
    EXPECT(parts.size() > 2, "Invalid URI: %s", uri.data());

    bucket_name = parts[2];
    path = "";
    for (size_t i = 3; i < parts.size(); ++i) {
        path += parts[i];
        if (i < parts.size() - 1) {
            path += "/";
        }
    }
}

bool
common::is_web_uri(std::string_view uri)
{
    return uri.starts_with("s3://") || uri.starts_with("http://") ||
           uri.starts_with("https://");
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

extern "C"
{
    acquire_export int unit_test__shard_index_for_chunk()
    {
        int retval = 0;
        try {
            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              64,
                              16, // 64 / 16 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              48,
                              16, // 48 / 16 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              6,
                              2,  // 6 / 2 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("c",
                              DimensionType_Channel,
                              8,
                              4,  // 8 / 4 = 2 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              5,  // 5 timepoints / chunk
                              2); // 2 chunks / shard

            CHECK(common::shard_index_for_chunk(0, dims) == 0);
            CHECK(common::shard_index_for_chunk(1, dims) == 0);
            CHECK(common::shard_index_for_chunk(2, dims) == 1);
            CHECK(common::shard_index_for_chunk(3, dims) == 1);
            CHECK(common::shard_index_for_chunk(4, dims) == 2);
            CHECK(common::shard_index_for_chunk(5, dims) == 2);
            CHECK(common::shard_index_for_chunk(6, dims) == 3);
            CHECK(common::shard_index_for_chunk(7, dims) == 3);
            CHECK(common::shard_index_for_chunk(8, dims) == 4);
            CHECK(common::shard_index_for_chunk(9, dims) == 4);
            CHECK(common::shard_index_for_chunk(10, dims) == 5);
            CHECK(common::shard_index_for_chunk(11, dims) == 5);
            CHECK(common::shard_index_for_chunk(12, dims) == 6);
            CHECK(common::shard_index_for_chunk(13, dims) == 6);
            CHECK(common::shard_index_for_chunk(14, dims) == 7);
            CHECK(common::shard_index_for_chunk(15, dims) == 7);
            CHECK(common::shard_index_for_chunk(16, dims) == 8);
            CHECK(common::shard_index_for_chunk(17, dims) == 8);
            CHECK(common::shard_index_for_chunk(18, dims) == 9);
            CHECK(common::shard_index_for_chunk(19, dims) == 9);
            CHECK(common::shard_index_for_chunk(20, dims) == 10);
            CHECK(common::shard_index_for_chunk(21, dims) == 10);
            CHECK(common::shard_index_for_chunk(22, dims) == 11);
            CHECK(common::shard_index_for_chunk(23, dims) == 11);
            CHECK(common::shard_index_for_chunk(24, dims) == 12);
            CHECK(common::shard_index_for_chunk(25, dims) == 12);
            CHECK(common::shard_index_for_chunk(26, dims) == 13);
            CHECK(common::shard_index_for_chunk(27, dims) == 13);
            CHECK(common::shard_index_for_chunk(28, dims) == 14);
            CHECK(common::shard_index_for_chunk(29, dims) == 14);
            CHECK(common::shard_index_for_chunk(30, dims) == 15);
            CHECK(common::shard_index_for_chunk(31, dims) == 15);
            CHECK(common::shard_index_for_chunk(32, dims) == 16);
            CHECK(common::shard_index_for_chunk(33, dims) == 16);
            CHECK(common::shard_index_for_chunk(34, dims) == 17);
            CHECK(common::shard_index_for_chunk(35, dims) == 17);
            CHECK(common::shard_index_for_chunk(36, dims) == 0);
            CHECK(common::shard_index_for_chunk(37, dims) == 0);
            CHECK(common::shard_index_for_chunk(38, dims) == 1);
            CHECK(common::shard_index_for_chunk(39, dims) == 1);
            CHECK(common::shard_index_for_chunk(40, dims) == 2);
            CHECK(common::shard_index_for_chunk(41, dims) == 2);
            CHECK(common::shard_index_for_chunk(42, dims) == 3);
            CHECK(common::shard_index_for_chunk(43, dims) == 3);
            CHECK(common::shard_index_for_chunk(44, dims) == 4);
            CHECK(common::shard_index_for_chunk(45, dims) == 4);
            CHECK(common::shard_index_for_chunk(46, dims) == 5);
            CHECK(common::shard_index_for_chunk(47, dims) == 5);
            CHECK(common::shard_index_for_chunk(48, dims) == 6);
            CHECK(common::shard_index_for_chunk(49, dims) == 6);
            CHECK(common::shard_index_for_chunk(50, dims) == 7);
            CHECK(common::shard_index_for_chunk(51, dims) == 7);
            CHECK(common::shard_index_for_chunk(52, dims) == 8);
            CHECK(common::shard_index_for_chunk(53, dims) == 8);
            CHECK(common::shard_index_for_chunk(54, dims) == 9);
            CHECK(common::shard_index_for_chunk(55, dims) == 9);
            CHECK(common::shard_index_for_chunk(56, dims) == 10);
            CHECK(common::shard_index_for_chunk(57, dims) == 10);
            CHECK(common::shard_index_for_chunk(58, dims) == 11);
            CHECK(common::shard_index_for_chunk(59, dims) == 11);
            CHECK(common::shard_index_for_chunk(60, dims) == 12);
            CHECK(common::shard_index_for_chunk(61, dims) == 12);
            CHECK(common::shard_index_for_chunk(62, dims) == 13);
            CHECK(common::shard_index_for_chunk(63, dims) == 13);
            CHECK(common::shard_index_for_chunk(64, dims) == 14);
            CHECK(common::shard_index_for_chunk(65, dims) == 14);
            CHECK(common::shard_index_for_chunk(66, dims) == 15);
            CHECK(common::shard_index_for_chunk(67, dims) == 15);
            CHECK(common::shard_index_for_chunk(68, dims) == 16);
            CHECK(common::shard_index_for_chunk(69, dims) == 16);
            CHECK(common::shard_index_for_chunk(70, dims) == 17);
            CHECK(common::shard_index_for_chunk(71, dims) == 17);
            CHECK(common::shard_index_for_chunk(72, dims) == 0);
            CHECK(common::shard_index_for_chunk(73, dims) == 0);
            CHECK(common::shard_index_for_chunk(74, dims) == 1);
            CHECK(common::shard_index_for_chunk(75, dims) == 1);
            CHECK(common::shard_index_for_chunk(76, dims) == 2);
            CHECK(common::shard_index_for_chunk(77, dims) == 2);
            CHECK(common::shard_index_for_chunk(78, dims) == 3);
            CHECK(common::shard_index_for_chunk(79, dims) == 3);
            CHECK(common::shard_index_for_chunk(80, dims) == 4);
            CHECK(common::shard_index_for_chunk(81, dims) == 4);
            CHECK(common::shard_index_for_chunk(82, dims) == 5);
            CHECK(common::shard_index_for_chunk(83, dims) == 5);
            CHECK(common::shard_index_for_chunk(84, dims) == 6);
            CHECK(common::shard_index_for_chunk(85, dims) == 6);
            CHECK(common::shard_index_for_chunk(86, dims) == 7);
            CHECK(common::shard_index_for_chunk(87, dims) == 7);
            CHECK(common::shard_index_for_chunk(88, dims) == 8);
            CHECK(common::shard_index_for_chunk(89, dims) == 8);
            CHECK(common::shard_index_for_chunk(90, dims) == 9);
            CHECK(common::shard_index_for_chunk(91, dims) == 9);
            CHECK(common::shard_index_for_chunk(92, dims) == 10);
            CHECK(common::shard_index_for_chunk(93, dims) == 10);
            CHECK(common::shard_index_for_chunk(94, dims) == 11);
            CHECK(common::shard_index_for_chunk(95, dims) == 11);
            CHECK(common::shard_index_for_chunk(96, dims) == 12);
            CHECK(common::shard_index_for_chunk(97, dims) == 12);
            CHECK(common::shard_index_for_chunk(98, dims) == 13);
            CHECK(common::shard_index_for_chunk(99, dims) == 13);
            CHECK(common::shard_index_for_chunk(100, dims) == 14);
            CHECK(common::shard_index_for_chunk(101, dims) == 14);
            CHECK(common::shard_index_for_chunk(102, dims) == 15);
            CHECK(common::shard_index_for_chunk(103, dims) == 15);
            CHECK(common::shard_index_for_chunk(104, dims) == 16);
            CHECK(common::shard_index_for_chunk(105, dims) == 16);
            CHECK(common::shard_index_for_chunk(106, dims) == 17);
            CHECK(common::shard_index_for_chunk(107, dims) == 17);
            CHECK(common::shard_index_for_chunk(108, dims) == 0);
            CHECK(common::shard_index_for_chunk(109, dims) == 0);
            CHECK(common::shard_index_for_chunk(110, dims) == 1);
            CHECK(common::shard_index_for_chunk(111, dims) == 1);
            CHECK(common::shard_index_for_chunk(112, dims) == 2);
            CHECK(common::shard_index_for_chunk(113, dims) == 2);
            CHECK(common::shard_index_for_chunk(114, dims) == 3);
            CHECK(common::shard_index_for_chunk(115, dims) == 3);
            CHECK(common::shard_index_for_chunk(116, dims) == 4);
            CHECK(common::shard_index_for_chunk(117, dims) == 4);
            CHECK(common::shard_index_for_chunk(118, dims) == 5);
            CHECK(common::shard_index_for_chunk(119, dims) == 5);
            CHECK(common::shard_index_for_chunk(120, dims) == 6);
            CHECK(common::shard_index_for_chunk(121, dims) == 6);
            CHECK(common::shard_index_for_chunk(122, dims) == 7);
            CHECK(common::shard_index_for_chunk(123, dims) == 7);
            CHECK(common::shard_index_for_chunk(124, dims) == 8);
            CHECK(common::shard_index_for_chunk(125, dims) == 8);
            CHECK(common::shard_index_for_chunk(126, dims) == 9);
            CHECK(common::shard_index_for_chunk(127, dims) == 9);
            CHECK(common::shard_index_for_chunk(128, dims) == 10);
            CHECK(common::shard_index_for_chunk(129, dims) == 10);
            CHECK(common::shard_index_for_chunk(130, dims) == 11);
            CHECK(common::shard_index_for_chunk(131, dims) == 11);
            CHECK(common::shard_index_for_chunk(132, dims) == 12);
            CHECK(common::shard_index_for_chunk(133, dims) == 12);
            CHECK(common::shard_index_for_chunk(134, dims) == 13);
            CHECK(common::shard_index_for_chunk(135, dims) == 13);
            CHECK(common::shard_index_for_chunk(136, dims) == 14);
            CHECK(common::shard_index_for_chunk(137, dims) == 14);
            CHECK(common::shard_index_for_chunk(138, dims) == 15);
            CHECK(common::shard_index_for_chunk(139, dims) == 15);
            CHECK(common::shard_index_for_chunk(140, dims) == 16);
            CHECK(common::shard_index_for_chunk(141, dims) == 16);
            CHECK(common::shard_index_for_chunk(142, dims) == 17);
            CHECK(common::shard_index_for_chunk(143, dims) == 17);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__shard_internal_index()
    {
        int retval = 0;

        std::vector<zarr::Dimension> dims;
        dims.emplace_back("x",
                          DimensionType_Space,
                          1080,
                          270, // 4 chunks
                          3);  // 2 ragged shards
        dims.emplace_back("y",
                          DimensionType_Space,
                          960,
                          320, // 3 chunks
                          2);  // 2 ragged shards
        dims.emplace_back("t",
                          DimensionType_Time,
                          0,
                          32, // 32 timepoints / chunk
                          1); // 1 shard

        try {
            CHECK(common::shard_index_for_chunk(0, dims) == 0);
            CHECK(common::shard_internal_index(0, dims) == 0);

            CHECK(common::shard_index_for_chunk(1, dims) == 0);
            CHECK(common::shard_internal_index(1, dims) == 1);

            CHECK(common::shard_index_for_chunk(2, dims) == 0);
            CHECK(common::shard_internal_index(2, dims) == 2);

            CHECK(common::shard_index_for_chunk(3, dims) == 1);
            CHECK(common::shard_internal_index(3, dims) == 0);

            CHECK(common::shard_index_for_chunk(4, dims) == 0);
            CHECK(common::shard_internal_index(4, dims) == 3);

            CHECK(common::shard_index_for_chunk(5, dims) == 0);
            CHECK(common::shard_internal_index(5, dims) == 4);

            CHECK(common::shard_index_for_chunk(6, dims) == 0);
            CHECK(common::shard_internal_index(6, dims) == 5);

            CHECK(common::shard_index_for_chunk(7, dims) == 1);
            CHECK(common::shard_internal_index(7, dims) == 3);

            CHECK(common::shard_index_for_chunk(8, dims) == 2);
            CHECK(common::shard_internal_index(8, dims) == 0);

            CHECK(common::shard_index_for_chunk(9, dims) == 2);
            CHECK(common::shard_internal_index(9, dims) == 1);

            CHECK(common::shard_index_for_chunk(10, dims) == 2);
            CHECK(common::shard_internal_index(10, dims) == 2);

            CHECK(common::shard_index_for_chunk(11, dims) == 3);
            CHECK(common::shard_internal_index(11, dims) == 0);
            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__split_uri()
    {
        try {
            auto parts = common::split_uri("s3://bucket/key");
            CHECK(parts.size() == 3);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");
            CHECK(parts[2] == "key");

            parts = common::split_uri("s3://bucket/key/");
            CHECK(parts.size() == 3);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");
            CHECK(parts[2] == "key");

            parts = common::split_uri("s3://bucket/key/with/slashes");
            CHECK(parts.size() == 5);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");
            CHECK(parts[2] == "key");
            CHECK(parts[3] == "with");
            CHECK(parts[4] == "slashes");

            parts = common::split_uri("s3://bucket");
            CHECK(parts.size() == 2);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");

            parts = common::split_uri("s3://");
            CHECK(parts.size() == 1);
            CHECK(parts[0] == "s3:");

            parts = common::split_uri("s3:///");
            CHECK(parts.size() == 1);
            CHECK(parts[0] == "s3:");

            parts = common::split_uri("s3://bucket/");
            CHECK(parts.size() == 2);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");

            parts = common::split_uri("s3://bucket/");
            CHECK(parts.size() == 2);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");

            parts = common::split_uri("s3://bucket/key/with/slashes/");
            CHECK(parts.size() == 5);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");
            CHECK(parts[2] == "key");
            CHECK(parts[3] == "with");
            CHECK(parts[4] == "slashes");

            parts = common::split_uri("s3://bucket/key/with/slashes//");
            CHECK(parts.size() == 5);
            CHECK(parts[0] == "s3:");
            CHECK(parts[1] == "bucket");
            CHECK(parts[2] == "key");
            CHECK(parts[3] == "with");
            CHECK(parts[4] == "slashes");
            return 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return 0;
    }
}
#endif
