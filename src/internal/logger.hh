#include <string>

enum LogLevel // todo (aliddell): Use enum class
{
    LogLevel_Debug,
    LogLevel_Info,
    LogLevel_Warning,
    LogLevel_Error
};

class Logger
{
  public:
    static void setLogLevel(LogLevel level);

    static LogLevel getLogLevel();

    static std::string log(LogLevel level,
                           const char* file,
                           int line,
                           const char* func,
                           const char* format,
                           ...);

  private:
    static LogLevel current_level;
};

#define LOG_DEBUG(...)                                                         \
    Logger::log(LogLevel_Debug, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
    Logger::log(LogLevel_Info, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_WARNING(...)                                                       \
    Logger::log(LogLevel_Warning, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
    Logger::log(LogLevel_Error, __FILE__, __LINE__, __func__, __VA_ARGS__)

#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            const std::string __err = LOG_ERROR(__VA_ARGS__);                  \
            throw std::runtime_error(__err);                                   \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)
