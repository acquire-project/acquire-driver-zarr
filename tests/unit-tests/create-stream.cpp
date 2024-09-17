#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrStreamSettings* settings)
{
    ZarrStreamSettings_reserve_dimensions(settings, 3);

    ZarrDimensionProperties dim = {
        .name = "t",
        .bytes_of_name = sizeof("t"),
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
    };
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 0, &dim),
              ZarrStatus_Success);

    dim.name = "y";
    dim.type = ZarrDimensionType_Space;
    dim.array_size_px = 200;
    dim.chunk_size_px = 20;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 1, &dim),
              ZarrStatus_Success);

    dim.name = "x";
    dim.array_size_px = 300;
    dim.chunk_size_px = 30;
    EXPECT_EQ(int,
              "%d",
              ZarrStreamSettings_set_dimension(settings, 2, &dim),
              ZarrStatus_Success);
}

int
main()
{
    int retval = 1;

    ZarrStream* stream;
    ZarrStreamSettings* settings = ZarrStreamSettings_create();
    if (!settings) {
        LOG_ERROR("Failed to create ZarrStreamSettings");
        return retval;
    }

    try {
        // try to create a stream with no store path
        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(nullptr == stream);

        // try to create a stream with no dimensions
        settings->store_path = TEST ".zarr";
        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(nullptr == stream);
        CHECK(!fs::exists(settings->store_path));

        configure_stream_dimensions(settings);
        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings->store_path));

        EXPECT_EQ(int, "%d", ZarrStream_get_version(stream), ZarrVersion_2);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR("%s", exception.what());
    }

    // cleanup
    if (fs::is_directory(settings->store_path)) {
        fs::remove_all(settings->store_path);
    }
    ZarrStreamSettings_destroy(settings);
    ZarrStream_destroy(stream);
    return retval;
}