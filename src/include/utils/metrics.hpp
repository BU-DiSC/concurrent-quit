#pragma once
#include <cstdint>
namespace utils::executor::metrics {
struct Latency {
    uint64_t preload = 0;
    uint64_t raw_writes = 0;
    uint64_t raw_reads = 0;
    uint64_t mixed = 0;
    uint64_t updates = 0;
    uint64_t short_range = 0;
    uint64_t mid_range = 0;
    uint64_t long_range = 0;
};
}  // namespace utils::executor::metrics