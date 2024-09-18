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
    Logger::log(ZarrLogLevel_Debug, __FILE__, __LINE__, __func__, __VA_ARGS__)
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
