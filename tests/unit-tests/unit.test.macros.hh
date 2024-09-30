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

/// Check that a==b
/// example: `ASSERT_EQ(int,42,meaning_of_life())`
#define EXPECT_EQ(T, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected ", #a, " == ", #b, " but ", a_, " != ", b_); \
    } while (0)

#define EXPECT_STR_EQ(a, b)                                                    \
    do {                                                                       \
        std::string a_ = (a) ? (a) : "";                                       \
        std::string b_ = (b) ? (b) : "";                                       \
        EXPECT(a_ == b_,                                                       \
               "Expected %s==%s but \"%s\"!=\"%s\"",                           \
               #a,                                                             \
               #b,                                                             \
               a_.c_str(),                                                     \
               b_.c_str());                                                    \
    } while (0)

#define EXPECT_INT_EQ(a, b)                                                    \
    EXPECT((a) == (b), "Expected ", #a, " == ", #b, ", but ", a, " != ", b)