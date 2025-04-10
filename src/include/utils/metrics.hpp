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

struct Stats {
    uint32_t num_leaf_nodes = 0;
    uint32_t num_internal_nodes = 0;
    uint32_t num_keys_inserted = 0;
    uint32_t num_fast_inserts = 0;
    uint32_t num_fast_inserts_fail = 0;
    uint32_t num_fastpath_resets = 0;
    uint32_t num_fastpath_sorts = 0;
};
}  // namespace utils::executor::metrics