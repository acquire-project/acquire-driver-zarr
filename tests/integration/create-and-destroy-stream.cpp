#include "zarr.h"

#include <cstdio>
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

// to be used in main()

#define SIZED(name) name, sizeof(name)

bool
set_valid_dimensions(ZarrStreamSettings* settings)
{
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 5, SIZED("x"), ZarrDimensionType_Space, 10, 5, 1),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 3, SIZED("y"), ZarrDimensionType_Space, 12, 3, 4),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 2, SIZED("t"), ZarrDimensionType_Time, 1, 1, 0),
             ZarrError_Success);
    return true;
Error:
    return false;
}

bool
set_invalid_dimensions(ZarrStreamSettings* settings)
{
    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 1, SIZED("y"), ZarrDimensionType_Space, 12, 3, 4),
             ZarrError_Success);

    CHECK_EQ(ZarrStreamSettings_set_dimension(
               settings, 2, SIZED("x"), ZarrDimensionType_Space, 1, 1, 1),
             ZarrError_Success);
    return true;

Error:
    return false;
}

int
main()
{
    int retval = 0;

    ZarrStreamSettings* settings;
    ZarrStream* stream;

    // test with invalid settings
    {
        settings = ZarrStreamSettings_create();
        CHECK(settings);

        CHECK(set_invalid_dimensions(settings));

        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(!stream);

        ZarrStream_destroy(stream); // also frees settings
    }

    // test with valid settings
    {
        settings = ZarrStreamSettings_create();
        CHECK(settings);

        CHECK(set_valid_dimensions(settings));

        const std::string store_path = TEST ".zarr";
        CHECK_EQ(ZarrStreamSettings_set_store_path(
                   settings, store_path.c_str(), store_path.size()),
                 ZarrError_Success);

        stream = ZarrStream_create(settings, ZarrVersion_2);
        CHECK(stream);

        ZarrStream_destroy(stream); // also frees settings
    }

Finalize:
    return retval;

Error:
    retval = 1;
    goto Finalize;
}