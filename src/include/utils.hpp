#pragma once
#include <config.hpp>
#include <fstream>
#include <vector>

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
Config load_configurations(std::string &config_file,
                           bool print_config = false) {
    Config conf;
    conf.parse(config_file.c_str());
    if (print_config) {
        conf.print();
    }
    return conf;
}

Config load_configurations(int argc, char **argv) {
    Config conf;
    conf.parse(argc, argv);
    // by default, print back config
    conf.print();
    return conf;
}
}  // namespace config

}  // namespace infra
}  // namespace utils
