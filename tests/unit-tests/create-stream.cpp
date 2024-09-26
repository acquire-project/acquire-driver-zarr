#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrStreamSettings* settings)
{
    CHECK(ZarrStatusCode_Success == ZarrStreamSettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties *dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
    };
}

int
main()
{
    int retval = 1;

    ZarrStream* stream;
    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));
    settings.version = ZarrVersion_2;

    try {
        // try to create a stream with no store path
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);

        // try to create a stream with no dimensions
        settings.store_path = static_cast<const char*>(TEST ".zarr");
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);
        CHECK(!fs::exists(settings.store_path));

        // allocate dimensions
        configure_stream_dimensions(&settings);
        stream = ZarrStream_create(&settings);
        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings.store_path));

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR("%s", exception.what());
    }

    // cleanup
    if (fs::is_directory(settings.store_path)) {
        fs::remove_all(settings.store_path);
    }
    ZarrStreamSettings_destroy_dimension_array(&settings);
    ZarrStream_destroy(stream);
    return retval;
}