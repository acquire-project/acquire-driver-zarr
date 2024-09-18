#include "logger.hh"

#include <cstdarg>
#include <iomanip>
#include <iostream>
#include <filesystem>
#include <string>
#include <thread>

ZarrLogLevel Logger::current_level_ = ZarrLogLevel_Info;
std::mutex Logger::log_mutex_{};

void
Logger::set_log_level(ZarrLogLevel level)
{
    current_level_ = level;
}

ZarrLogLevel
Logger::get_log_level()
{
    return current_level_;
}

std::string
Logger::log(ZarrLogLevel level,
            const char* file,
            int line,
            const char* func,
            const char* format,
            ...)
{
    std::scoped_lock lock(log_mutex_);
    if (current_level_ == ZarrLogLevel_None || level < current_level_) {
        return {}; // Suppress logs
    }

    va_list args;
    va_start(args, format);

    std::string prefix;
    std::ostream* stream = &std::cout;

    switch (level) {
        case ZarrLogLevel_Debug:
            prefix = "[DEBUG] ";
            break;
        case ZarrLogLevel_Info:
            prefix = "[INFO] ";
            break;
        case ZarrLogLevel_Warning:
            prefix = "[WARNING] ";
            stream = &std::cerr;
            break;
        case ZarrLogLevel_Error:
            prefix = "[ERROR] ";
            stream = &std::cerr;
            break;
    }

    // Get current time
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
              1000;

    // Get filename without path
    std::filesystem::path filepath(file);
    std::string filename = filepath.filename().string();

    // Output timestamp, log level, filename
    *stream << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S") << '.'
            << std::setfill('0') << std::setw(3) << ms.count() << " " << prefix
            << filename << ":" << line << " " << func << ": ";

    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, args);
    *stream << buffer << std::endl;

    va_end(args);

    return buffer;
}