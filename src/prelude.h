/// Do not include in public headers.
/// Contains common macros used for logging/error handling with this module

#ifndef H_ACQUIRE_STORAGE_ZARR_PRELUDE_V0
#define H_ACQUIRE_STORAGE_ZARR_PRELUDE_V0

#include "logger.h"

#include <stdexcept>

#define LOG(...) aq_logger(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) aq_logger(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Expression was false: " #e);             \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

//#define TRACE(...) LOG(__VA_ARGS__)  // TODO (aliddell): switch this back
#define TRACE(...)

#define containerof(ptr, T, V) ((T*)(((char*)(ptr)) - offsetof(T, V)))
#define countof(e) (sizeof(e) / sizeof(*(e)))

#endif // H_ACQUIRE_STORAGE_ZARR_PRELUDE_V0
