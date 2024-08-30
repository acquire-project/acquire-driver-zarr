#include "stream.settings.hh"
#include "zarr.h"

#include <cstdio>

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "Check failed: %s\n", #cond);                      \
            goto Error;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

int
main()
{
    ZarrDimension_s dimension = {
        .kind = ZarrDimensionTypeCount,
        .array_size_px = 0,
        .chunk_size_px = 0,
        .shard_size_chunks = 0,
    };

    int retval = 0;

    // dimension is not valid because name is invalid
    CHECK(!validate_dimension(dimension));
    dimension.name = "x";

    // dimension is not valid because kind is invalid
    CHECK(!validate_dimension(dimension));
    dimension.kind = ZarrDimensionType_Space;

    // dimension is not valid because chunk_size_px is invalid
    CHECK(!validate_dimension(dimension));
    dimension.chunk_size_px = 1;

    // dimension is valid
    CHECK(validate_dimension(dimension));

Finalize:
    return retval;

Error:
    retval = 1;
    goto Finalize;
}