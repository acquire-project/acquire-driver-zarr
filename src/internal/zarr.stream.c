#include <stdlib.h> // malloc, free
#include <string.h>

#include "zarr.stream.h"
#include "zarr.h"

static ZarrError
ZarrStreamSettings_defragment_dims(struct ZarrStream_dimensions_s* dimensions)
{
    if (!dimensions)
        return ZarrError_InvalidArgument;

    const size_t max_dims =
      sizeof(dimensions->data) / sizeof(dimensions->data[0]);
    size_t count = 0;
    size_t first_empty = 0;

    // move all non-empty slots to the front
    while (count < dimensions->count) {
        // search for the first empty slot
        for (size_t i = first_empty; i < max_dims; ++i) {
            if (dimensions->data[i].name[0] == '\0') {
                first_empty = i;
                break;
            }
        }

        struct ZarrDimension_s* empty = &dimensions->data[first_empty];

        // search for the first non-empty slot after the empty slot
        for (size_t i = first_empty + 1; i < max_dims; ++i) {
            if (dimensions->data[i].name[0] != '\0') {
                struct ZarrDimension_s* nonempty = &dimensions->data[i];
                memcpy(empty, nonempty, sizeof(*empty));
                memset(nonempty, 0, sizeof(*nonempty));
                ++first_empty;
                ++count;
                break;
            }
        }
    }

    // verify that the dimensions are contiguous
    for (size_t i = 0; i < dimensions->count; ++i) {
        if (dimensions->data[i].name[0] == '\0') {
            return ZarrError_Failure;
        }
    }
    for (size_t i = dimensions->count; i < max_dims; ++i) {
        if (dimensions->data[i].name[0] != '\0') {
            return ZarrError_Failure;
        }
    }

    return ZarrError_Success;
}

ZarrStream*
ZarrStream_create(ZarrStreamSettings* settings, ZarrVersion version)
{
    if (!settings || version < ZarrVersion_2 || version >= ZarrVersionCount)
        return 0;

    // we require at least 3 dimensions
    if (settings->dimensions.count < 3)
        return 0;

    // defragment and validate dimensions
    if (ZarrStreamSettings_defragment_dims(&settings->dimensions) !=
        ZarrError_Success)
        return 0;

    for (size_t i = 0; i < settings->dimensions.count; ++i) {
        if (!validate_dimension(&settings->dimensions.data[i]))
            return 0;
    }

    ZarrStream* stream = malloc(sizeof(ZarrStream));
    if (!stream)
        return 0;

    memset(stream, 0, sizeof(*stream));
    stream->settings = settings;
    stream->version = version;

    return stream;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    if (!stream)
        return;

    ZarrStreamSettings_destroy(stream->settings);
    free(stream);
}