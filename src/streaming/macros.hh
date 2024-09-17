#pragma once

#include "logger.hh"

#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            const std::string __err = LOG_ERROR(__VA_ARGS__);                  \
            throw std::runtime_error(__err);                                   \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

#define EXPECT_VALID_ARGUMENT(e, ...)                                          \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrStatus_InvalidArgument;                                 \
        }                                                                      \
    } while (0)

#define EXPECT_VALID_INDEX(e, ...)                                             \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOG_ERROR(__VA_ARGS__);                                            \
            return ZarrStatus_InvalidIndex;                                    \
        }                                                                      \
    } while (0)