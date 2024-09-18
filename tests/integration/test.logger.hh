#include "zarr.h"

#include <string>

class Logger
{
  public:
    static void set_log_level(ZarrLogLevel level);
    static ZarrLogLevel get_log_level();

    static std::string log(ZarrLogLevel level,
                           const char* file,
                           int line,
                           const char* func,
                           const char* format,
                           ...);

  private:
    static ZarrLogLevel current_level;
};

#define LOG_DEBUG(...)                                                         \
    Logger::log(LogLevel_Debug, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
    Logger::log(LogLevel_Info, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_WARNING(...)                                                       \
    Logger::log(ZarrLogLevel_Warning, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
    Logger::log(ZarrLogLevel_Error, __FILE__, __LINE__, __func__, __VA_ARGS__)

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

#define EXPECT_LT(T, fmt, a, b)                                                \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(a_ < b_, "Expected %s<%s but " fmt ">=" fmt, #a, #b, a_, b_);   \
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
