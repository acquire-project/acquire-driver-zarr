#include "zarr.types.h"

#include <mutex>

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
    static ZarrLogLevel current_level_;
    static std::mutex log_mutex_;
};

#define LOG_DEBUG(...)                                                         \
      Logger::log(ZarrLogLevel_Debug, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
    Logger::log(LogLevel_Info, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_WARNING(...)                                                       \
    Logger::log(ZarrLogLevel_Warning, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
    Logger::log(ZarrLogLevel_Error, __FILE__, __LINE__, __func__, __VA_ARGS__)
