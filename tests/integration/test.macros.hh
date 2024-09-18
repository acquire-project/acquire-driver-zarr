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
/// example: `ASSERT_EQ(int,"%d",42,meaning_of_life())`
#define EXPECT_EQ(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ == b_, "Expected %s==%s but " fmt "!=" fmt, #a, #b, a_, b_); \
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

#define EXPECT_LT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ < b_, "Expected %s<%s but " fmt ">=" fmt, #a, #b, a_, b_);   \
    } while (0)

#define SIZED(str) str, sizeof(str)
#define DIM(name_, type_, array_size, chunk_size, shard_size)                  \
    {                                                                          \
        .name = (name_), .bytes_of_name = sizeof((name_)), .type = (type_),          \
        .array_size_px = (array_size), .chunk_size_px = (chunk_size),              \
        .shard_size_chunks = (shard_size)                                        \
    }