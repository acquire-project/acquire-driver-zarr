#include "logger.hh"

#include <iostream>
#include <cstdarg>
#include <chrono>
#include <iomanip>
#include <filesystem>

LogLevel Logger::current_level = LogLevel_Info;

void
Logger::setLogLevel(LogLevel level)
{
    current_level = level;
}

LogLevel
Logger::getLogLevel()
{
    return current_level;
}

std::string
Logger::log(LogLevel level,
            const char* file,
            int line,
            const char* func,
            const char* format,
            ...)
{
    if (level < current_level)
        return {}; // Suppress logs below current_level

    va_list args;
    va_start(args, format);

    std::string prefix;
    std::ostream* stream = &std::cout;

    switch (level) {
        case LogLevel_Debug:
            prefix = "[DEBUG] ";
            break;
        case LogLevel_Info:
            prefix = "[INFO] ";
            break;
        case LogLevel_Warning:
            prefix = "[WARNING] ";
            stream = &std::cerr;
            break;
        case LogLevel_Error:
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