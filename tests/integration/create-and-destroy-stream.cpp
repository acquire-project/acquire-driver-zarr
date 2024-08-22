#include "zarr.h"

#include <cstdio>
#include <filesystem>
#include <string>

// to be used in functions returning bool
#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "Assertion failed: %s\n", #cond);                  \
            goto Error;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

#define SIZED(name) name, sizeof(name)

namespace fs = std::filesystem;

bool
try_with_invalid_settings()
{
    ZarrStreamSettings* settings;
    ZarrStream* stream;

    settings = ZarrStreamSettings_create();
    CHECK(settings);

    // reserve 3 dimensions, but only set 2 of them
    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 3),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 1, SIZED("y"), ZarrDimensionType_Space, 12, 3, 4),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 2, SIZED("x"), ZarrDimensionType_Space, 1, 1, 1),
             ZarrError_Success);

    stream = ZarrStream_create(settings, ZarrVersion_2);
    CHECK(!stream);

    return true;

Error:
    return false;
}

bool
try_with_valid_settings()
{
    ZarrStreamSettings* settings;
    ZarrStream* stream;
    const std::string store_path = TEST ".zarr";

    settings = ZarrStreamSettings_create();
    CHECK(settings);

    CHECK_EQ(ZarrStreamSettings_reserve_dimensions(settings, 3),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 2, SIZED("x"), ZarrDimensionType_Space, 10, 5, 1),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 1, SIZED("y"), ZarrDimensionType_Space, 12, 3, 4),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 0, SIZED("t"), ZarrDimensionType_Time, 1, 1, 0),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_store_path(
               settings, store_path.c_str(), store_path.size()),
             ZarrError_Success);

    stream = ZarrStream_create(settings, ZarrVersion_2);
    CHECK(stream);

    // check the store path was created
    CHECK(fs::is_directory(store_path));

    // cleanup
    try {
        fs::remove_all(store_path);
    } catch (const fs::filesystem_error& e) {
        fprintf(
          stderr, "Failed to remove %s: %s\n", store_path.c_str(), e.what());
        ZarrStream_destroy(stream);
        return false;
    }

    ZarrStream_destroy(stream);

    return true;

Error:
    return false;
}

int
main()
{
    int retval = 0;

    CHECK(try_with_invalid_settings());
    CHECK(try_with_valid_settings());

Finalize:
    return retval;

Error:
    retval = 1;
    goto Finalize;
}