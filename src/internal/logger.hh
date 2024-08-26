#include <string>

enum class LogLevel
{
    DEBUG,
    INFO,
    WARNING,
    ERROR
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
    Logger::log(LogLevel::DEBUG, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
    Logger::log(LogLevel::INFO, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_WARNING(...)                                                       \
    Logger::log(LogLevel::WARNING, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
    Logger::log(LogLevel::ERROR, __FILE__, __LINE__, __func__, __VA_ARGS__)

#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e))                                                              \
            throw std::runtime_error(LOG_ERROR(__VA_ARGS__));                  \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)
