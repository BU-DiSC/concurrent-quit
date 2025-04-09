#pragma once

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string_view>

namespace utils::logging {
class Logger {
   private:
    std::shared_ptr<spdlog::logger> logger;

   public:
    Logger(const std::string& name) : logger(spdlog::get(name)) {
        if (!logger) {
            logger = spdlog::stdout_color_mt(name);
        }
        logger->set_level(spdlog::level::trace);  // Set log level to trace
    }

    template <typename... Args>
    Logger& info(const std::string& format, Args&&... args) {
        logger->info(fmt::runtime(format), std::forward<Args>(args)...);
        return *this;
    }

    template <typename... Args>
    Logger& trace(const std::string& format, Args&&... args) {
        logger->trace(fmt::runtime(format), std::forward<Args>(args)...);
        return *this;
    }

    template <typename... Args>
    Logger& error(const std::string& format, Args&&... args) {
        logger->error(fmt::runtime(format), std::forward<Args>(args)...);
        return *this;
    }
};
}  // namespace utils::logging