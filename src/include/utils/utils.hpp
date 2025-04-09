#pragma once
#include <filesystem>
#include <fstream>
#include <vector>

#include "config.hpp"
#include "logging.hpp"

namespace utils {
namespace infra {
namespace file_ops {
template <typename key_type>
std::vector<key_type> read_txt(const char *filename) {
    std::vector<key_type> data;
    std::string line;
    std::ifstream ifs(filename);
    while (std::getline(ifs, line)) {
        key_type key = std::stoul(line);
        data.push_back(key);
    }
    return data;
}

template <typename key_type>
std::vector<key_type> read_bin(const char *filename) {
    std::ifstream inputFile(filename, std::ios::binary);
    assert(inputFile.is_open());
    inputFile.seekg(0, std::ios::end);
    const std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<key_type> data(fileSize / sizeof(key_type));
    inputFile.read(reinterpret_cast<char *>(data.data()), fileSize);
    return data;
}
};  // namespace file_ops

namespace config {
void load_configurations(Config &conf, std::string &config_file) {
    conf.parse(config_file.c_str());
}

void load_configurations(Config &conf, int argc, char **argv) {
    conf.parse(argc, argv);
}

void print_configurations(Config &conf) {
    auto &log = utils::logging::Logger::get_instance();
    if (conf.verbose) conf.print(log);
}

}  // namespace config
namespace load {
template <typename key_type>
void load_data(std::vector<std::vector<key_type>> &data, Config &conf) {
    for (const auto &file : conf.files) {
        auto &log = utils::logging::Logger::get_instance();
        std::filesystem::path fsPath(file);
        if (conf.verbose) {
            log.trace("Reading {}", fsPath.filename().c_str());
        }
        if (conf.binary_input) {
            data.emplace_back(utils::infra::file_ops::read_bin<key_type>(file));
        } else {
            data.emplace_back(utils::infra::file_ops::read_txt<key_type>(file));
        }
    }
}
}  // namespace load
}  // namespace infra
}  // namespace utils
