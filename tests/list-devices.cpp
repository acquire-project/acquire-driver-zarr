#include "acquire.h"
#include "device/hal/device.manager.h"
#include "logger.h"

#include <cstdio>
#include <stdexcept>

#define L (aq_logger)
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define ERR(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define CHECK(e)                                                               \
    do {                                                                       \
        if (!(e)) {                                                            \
            ERR("Expression was false:\n\t%s\n", #e);                          \
            throw std::runtime_error("Expression was false: " #e);             \
        }                                                                      \
    } while (0)

void
reporter(int is_error,
         const char* file,
         int line,
         const char* function,
         const char* msg)
{
    printf("%s%s(%d) - %s: %s\n",
           is_error ? "ERROR " : "",
           file,
           line,
           function,
           msg);
}

int
main(int n, char** args)
{
    auto runtime = acquire_init(reporter);
    CHECK(runtime);
    auto device_manager = acquire_device_manager(runtime);
    CHECK(device_manager);
    for (uint32_t i = 0; i < device_manager_count(device_manager); ++i) {
        struct DeviceIdentifier identifier = {};
        CHECK(Device_Ok == device_manager_get(&identifier, device_manager, i));
        CHECK(identifier.kind < DeviceKind_Count);
        printf("%3d - %10s %s\n",
               (int)i,
               device_kind_as_string(identifier.kind),
               identifier.name);
    }
    acquire_shutdown(runtime);
    return 0;
}
