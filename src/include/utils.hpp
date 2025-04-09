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
void load_configurations(Config &conf, std::string &config_file) {
    conf.parse(config_file.c_str());
}

void load_configurations(Config &conf, int argc, char **argv) {
    conf.parse(argc, argv);
}

void print_configurations(Config &conf) {
    if (conf.verbose) conf.print();
}
}  // namespace config

}  // namespace infra
}  // namespace utils
