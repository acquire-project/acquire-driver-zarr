#include "utilities.hh"
#include "driver/zarr.hh"

#include "platform.h"
#include "thread.pool.hh"

#include <cmath>
#include <thread>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

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
