#include "device/kit/driver.h"
#include "device/kit/storage.h"
#include "logger.h"

#include <string.h>
#include <stdlib.h>

#define containerof(P, T, F) ((T*)(((char*)(P)) - offsetof(T, F)))

#define L (aq_logger)
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define ERR(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define CHECK(e)                                                               \
    do {                                                                       \
        if (!(e)) {                                                            \
            ERR("Expression was false:\n\t%s\n", #e);                          \
            goto Error;                                                        \
        }                                                                      \
    } while (0)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            ERR(__VA_ARGS__);                                                  \
            goto Error;                                                        \
        }                                                                      \
    } while (0)

//
//                  EXTERN
//

// Each of these allocates a storage object on `init`. This should happen in
// `storage_open()`.
//
// The deallocate themselves when their `destroy()` method is called.
struct Storage*
zarr_init();
struct Storage*
compressed_zarr_zstd_init();
struct Storage*
compressed_zarr_lz4_init();

//
//                  GLOBALS
//

enum StorageKind
{
    Storage_Zarr,
    Storage_ZarrBlosc1ZstdByteShuffle,
    Storage_ZarrBlosc1Lz4ByteShuffle,
    Storage_Number_Of_Kinds
};

static struct
{
    struct Storage* (**constructors)();
} globals = { 0 };

//
//                  IMPL
//

static const char*
storage_kind_to_string(const enum StorageKind kind)
{
    switch (kind) {
#define CASE(e)                                                                \
    case e:                                                                    \
        return #e
        CASE(Storage_Zarr);
        CASE(Storage_ZarrBlosc1ZstdByteShuffle);
        CASE(Storage_ZarrBlosc1Lz4ByteShuffle);
#undef CASE
        default:
            return "(unknown)";
    }
}

static uint32_t
zarr_count(struct Driver* driver)
{
    return Storage_Number_Of_Kinds;
}

static enum DeviceStatusCode
zarr_describe(const struct Driver* driver,
              struct DeviceIdentifier* identifier,
              uint64_t i)
{
#define XXX(N)                                                                 \
    [Storage_##N] = {                                                          \
        .device_id = Storage_##N,                                              \
        .kind = DeviceKind_Storage,                                            \
        .name = #N,                                                            \
    }
    // clang-format off
    static struct DeviceIdentifier identifiers[] = {
        XXX(Zarr),
        XXX(ZarrBlosc1ZstdByteShuffle),
        XXX(ZarrBlosc1Lz4ByteShuffle),
    };
    // clang-format on
#undef XXX
    CHECK(i < Storage_Number_Of_Kinds);
    memcpy(identifier, identifiers + i, sizeof(*identifier));
    return Device_Ok;
Error:
    return Device_Err;
}

static enum DeviceStatusCode
zarr_open(struct Driver* driver, uint64_t device_id, struct Device** out)
{
    struct Storage* storage = 0;
    EXPECT(
      device_id < Storage_Number_Of_Kinds, "Invalid device id %d", device_id);
    EXPECT(out, "Invalid parameter. out was NULL.");
    EXPECT(storage = globals.constructors[device_id](),
           "Storage device (%s) not supported",
           storage_kind_to_string(device_id));

    *out = &storage->device;
    return Device_Ok;
Error:
    if (storage) {
        storage->destroy(storage);
    }
    return Device_Err;
}

static enum DeviceStatusCode
zarr_close(struct Driver* driver, struct Device* in)
{
    EXPECT(in, "Invalid parameter. Received NULL.");
    struct Storage* writer = containerof(in, struct Storage, device);
    writer->destroy(writer);
    return Device_Ok;
Error:
    return Device_Err;
}

static enum DeviceStatusCode
zarr_shutdown(struct Driver* driver)
{
    free(globals.constructors);
    globals.constructors = 0;
    return Device_Ok;
}

acquire_export struct Driver*
acquire_driver_init_v0(acquire_reporter_t reporter)
{
    logger_set_reporter(reporter);
    {
        const size_t nbytes =
          sizeof(globals.constructors[0]) * Storage_Number_Of_Kinds;
        CHECK(globals.constructors = (struct Storage * (**)()) malloc(nbytes));
        struct Storage* (*impls[])() = {
            [Storage_Zarr] = zarr_init,
            [Storage_ZarrBlosc1ZstdByteShuffle] = compressed_zarr_zstd_init,
            [Storage_ZarrBlosc1Lz4ByteShuffle] = compressed_zarr_lz4_init,
        };
        memcpy(
          globals.constructors, impls, nbytes); // cppcheck-suppress uninitvar
    }

    static struct Driver driver = {
        .open = zarr_open,
        .shutdown = zarr_shutdown,
        .close = zarr_close,
        .describe = zarr_describe,
        .device_count = zarr_count,
    };

    return &driver;

Error:
    return 0;
}
